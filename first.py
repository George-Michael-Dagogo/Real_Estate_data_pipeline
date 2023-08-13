import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient
path = '../Real_Estate_data_pipeline/property_csv'

def extract_data():
    for i in os.listdir(path):
        os.remove(path +'/' + i)
    subprocess.run('''python extract_transform_scripts/for_rent.py & 
    python extract_transform_scripts/for_sale.py & 
    python extract_transform_scripts/short_let.py''', shell=True)

def load_blob_storage(connection_string, container_name, source_directory):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # List all files in the source directory
    for file in os.listdir(source_directory):
        file_path = os.path.join(source_directory, file)

        blob_client = container_client.get_blob_client(file)

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        print(f" '{file_path}' uploaded to Blob Storage.")

extract_data()
store=''
load_blob_storage(connection_string = store, container_name = 'testtech', source_directory = '../Real_Estate_data_pipeline/property_csv' )