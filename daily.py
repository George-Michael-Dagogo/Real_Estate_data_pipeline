import subprocess
import os
import pandas as pd
import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(days=+1)
yesterday_= '_' + str(yesterday)

path = '../Real_Estate_data_pipeline_NG/property_csv'


def extract_data():
    for i in os.listdir(path):
        os.remove(path +'/' + i)

    os.system('python daily_scripts/for_rent.py')
    os.system('python daily_scripts/for_sale.py')
    os.system('python daily_scripts/short_let.py')


def merge_csv():

    csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]

    dfs = []
    for csv_file in csv_files:
        csv_file_path = os.path.join(path, csv_file)
        df = pd.read_csv(csv_file_path)
        dfs.append(df)
        os.remove(csv_file_path)

    merged_df = pd.concat(dfs, ignore_index=True) 
    merged_df.to_csv(f'../Real_Estate_data_pipeline_NG/property_csv/propertypro_merged{yesterday_}.csv', index=False)


def upload_ADLS():
    storage_account_name='testtechmichael'
    storage_account_key='L9KwVgy28xG0vrcvWHA3Pb7uJFeclYGtj6u2NYA0/oYT24+TQGE8XnuIvEOlea1qVdmnvNIokPgf+AStsxUFtw=='

    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)
        
    file_system_client = service_client.get_file_system_client(file_system="testtech")

    directory_client = file_system_client.get_directory_client(directory="testtech")
            
    file_client = directory_client.create_file(f"propertypro_merged{yesterday_}.csv")

    local_file = pd.read_csv(f"../Real_Estate_data_pipeline/property_csv/propertypro_merged{yesterday_}.csv")
    df = pd.DataFrame(local_file).to_csv()

    file_client.upload_data(data=df,overwrite=True) #Either of the lines works
    file_client.append_data(data=df, offset=0, length=len(df)) 
    #file_client.flush_data(len(df))

extract_data()
merge_csv()
upload_ADLS()