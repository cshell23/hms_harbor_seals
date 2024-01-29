import os
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError
from google.cloud import storage

# Dropbox setup
TOKEN = "gpmihcn9klwx22q"   
dbx = dropbox.Dropbox(TOKEN)
dropbox_folder_path = 'path/to/your/folder'

# Google Cloud Storage setup
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(
        'path_to_your_service_account_file.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

# Local directory setup
local_folder_path = '/path/to/local/folder'

# Set batch size
batch_size = 100

try:
    # Get the list of files in the Dropbox folder
    entries = dbx.files_list_folder(dropbox_folder_path).entries

    # Process the files in batches
    for i in range(0, len(entries), batch_size):
        batch = entries[i:i+batch_size]

        # Download batch of files to local VM
        for entry in batch:
            dbx.files_download_to_file(local_folder_path + '/' + entry.name, dropbox_folder_path + '/' + entry.name)

        # Upload batch of files to GCS
        for filename in os.listdir(local_folder_path):
            source_file_name = local_folder_path + '/' + filename
            destination_blob_name = 'path/in/gcs/bucket/' + filename
            upload_blob('your-bucket-name', source_file_name, destination_blob_name)

        # Optional: remove local files after upload to save space
        for filename in os.listdir(local_folder_path):
            os.remove(local_folder_path + '/' + filename)

except Exception as e:
    print("Failed to download or upload batch of files: ", e)

