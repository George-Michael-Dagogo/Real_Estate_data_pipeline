import subprocess
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
import datetime

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(days=+1)
yesterday_= '_' + str(yesterday)

path = '../Real_Estate_data_pipeline/property_csv'

def extract_data():
    for i in os.listdir(path):
        os.remove(path +'/' + i)

    subprocess.run('''python daily_scripts/for_rent.py & 
    python daily_scripts/for_sale.py & 
    python daily_scripts/short_let.py''', shell=True)


def merge_csv():

    csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]

    dfs = []
    for csv_file in csv_files:
        csv_file_path = os.path.join(path, csv_file)
        df = pd.read_csv(csv_file_path)
        dfs.append(df)
        os.remove(csv_file_path)

    merged_df = pd.concat(dfs, ignore_index=True) 
    merged_df.to_csv(f'../Real_Estate_data_pipeline/property_csv/propertypro_merged{yesterday_}.csv', index=False)





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

#extract_data()
#merge_csv()
store=''
load_blob_storage(connection_string = store, container_name = 'testtech', source_directory = '../Real_Estate_data_pipeline/property_csv' )