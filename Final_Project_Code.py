import os
import re
import PyPDF2
import psycopg2
import io
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Set up logging
logging.basicConfig(filename='app.log', level=logging.INFO)

# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname="Diagnostic",
        user="postgres",
        password="shubham",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
except psycopg2.Error as e:
    logging.error("Error connecting to PostgreSQL:", e)
    raise

# Patterns for data extraction
pattern1 = 'Version '
pattern2 = 'LoggedInUsersCount '
pattern3 = 'TotalUsersCount '

# Service account credentials for accessing Google Drive
SERVICE_ACCOUNT_FILE = 'C://Users//sudata//Desktop//Report_Analysis_Project//Google_API_Key//credentials.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


# Function to extract next word after pattern matching
def extract_next_word_from_pattern(pdf_content, pattern):
    extracted_word = None
    try:
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
        for page_number in range(len(pdf_reader.pages)):
            text = pdf_reader.pages[page_number].extract_text()
            match = re.search(pattern, text)
            if match:
                match_end_index = match.end()
                next_word_match = re.search(r'\b\w+\b', text[match_end_index:])
                if next_word_match:
                    extracted_word = next_word_match.group()
                break
    except Exception as e:
        logging.error("Error extracting data: {e}")
    return extracted_word


# Function to store data in PostgreSQL
def store_data_in_postgres(pdf_filename, word1, word2, word3):
    try:
        # Remove ".pdf" extension from filename
        pdf_filename = os.path.splitext(pdf_filename)[0]
        cursor.execute(
            "INSERT INTO customertest(customer_name, jrs_version, loggedin_user_count, total_user_count) "
            "VALUES (%s, %s, %s, %s)",
            (pdf_filename, word1, word2, word3))
        conn.commit()
        logging.info(f"Data from {pdf_filename} stored successfully in PostgreSQL!")
    except psycopg2.Error as e:
        logging.error("Error inserting data from {pdf_filename} into PostgreSQL:", e)



# Function to download PDF file from Google Drive
def download_pdf_from_drive(drive_service, file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        return fh.getvalue()
    except Exception as e:
        logging.error(f"Error downloading PDF from Google Drive: {e}")
        return None


# Authenticate with Google Drive API
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)


# Function to retrieve all files in a folder
def list_files_in_folder(drive_service, folder_id):
    try:
        response = drive_service.files().list(q=f"'{folder_id}' in parents", fields='files(id, name)').execute()
        files = response.get('files', [])
        return files
    except Exception as e:
        logging.error(f"Error listing files in folder: {e}")
        return []


# Folder ID of the Google Drive folder containing the PDF files
FOLDER_ID = '1p949jyarEFjiBjbpW27xLGlpoQiGiVAO'  # Replace 'your_folder_id' with the actual folder ID

# Retrieve all files in the specified folder
files_in_folder = list_files_in_folder(drive_service, FOLDER_ID)

# Iterate through each file in the folder
for file_info in files_in_folder:
    file_id = file_info['id']
    file_name = file_info['name']

    pdf_content = download_pdf_from_drive(drive_service, file_id)
    if pdf_content:
        word1 = extract_next_word_from_pattern(pdf_content, pattern1)
        word2 = extract_next_word_from_pattern(pdf_content, pattern2)
        word3 = extract_next_word_from_pattern(pdf_content, pattern3)

        store_data_in_postgres(file_name, word1, word2, word3)

# Close database connection
try:
    cursor.close()
    conn.close()
except psycopg2.Error as e:
    logging.error("Error closing database connection:", e)
    raise
