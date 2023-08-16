import pysftp
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
from azure.identity import (DefaultAzureCredential, ClientSecretCredential)
from azure.storage.filedatalake import (
                            DataLakeServiceClient,
                            DataLakeDirectoryClient,
                            FileSystemClient)
import configparser


class process_ftp_file():
    def __init__(self, d_datalake_config:dict, d_ftp_config:dict): 
        ##intialize ftp configuration
        self.host_name = d_ftp_config['HOSTNAME']
        self.user_name = d_ftp_config['USERNAME'] 
        self.password = d_ftp_config['PASSWORD']
        self.ftp_dir = d_ftp_config['HOME_DIR']   

        ##initialize datalake configuration
        self.client_id = d_datalake_config['CLIENT_ID']
        self.client_secret = d_datalake_config['CLIENT_SECRET'] 
        self.tenant_id = d_datalake_config['TENANT_ID']
        self.account_url = d_datalake_config['ACCOUNT_URL']

    def read_ftp_files(self, cnopts):
        pdf = pd.DataFrame()
        try:
            with pysftp.Connection(host=self.host_name,
                                   username=self.user_name,
                                   password=self.password, 
                                   cnopts=cnopts) as sftp:
                print ("Connection succesfully stablished ... ")
        
                directory_structure = sftp.listdir_attr(self.ftp_dir)
                for attr in directory_structure:    
            
                    print("Reading file content : ",attr.filename)
                    fullPath = self.ftp_dir+"/"+attr.filename

                    with sftp.open(fullPath,"rb") as file:
                        pdf = pd.read_csv(file, sep="\t",header=1)                        
                return pdf
            print("Reading data from FTP successful")
        except:
            raise

    def write_to_datalake(self, read_df, adls_container, adls_location, file_name ):
                
        token_credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        adls_service_client = DataLakeServiceClient(self.account_url, credential=token_credential)        

        write_df = read_df.to_csv(index=False, encoding="utf-8 sig")

        file_system_client = adls_service_client.get_file_system_client(file_system=adls_container)
        dir_client = file_system_client.get_directory_client(adls_location)
        file_client = dir_client.get_file_client(file_name)

        file_client.upload_data(write_df, overwrite=True)
        print("Writing data to adls successful")
if __name__ == "__main__":
    ##Set hostkeys to none 
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    
    ##Initialize config parser and read config.ini file
    config = configparser.ConfigParser()
    config.read("config.ini")

    ## Initialize dictionary with config.ini ftp configuration
    dict_ftp = {'HOSTNAME':config['FTP']['HOSTNAME'],
                'USERNAME':config['FTP']['USERNAME'],
                'PASSWORD':config['FTP']['PASSWORD'],
                'HOME_DIR':config['FTP']['HOME_DIR']
               }
    
    ## Initialize dictionary with config.ini adls configuration
    dict_adls = {'CLIENT_ID':config['ADLS']['CLIENT_ID'],
                'CLIENT_SECRET':config['ADLS']['CLIENT_SECRET'],
                'TENANT_ID':config['ADLS']['TENANT_ID'],
                'ACCOUNT_URL':config['ADLS']['ACCOUNT_URL']
               }
    
    ## Initialize the ADLS location and container
    ADLS_CONTAINER = "container"
    ADLS_LOCATION = "bronze/test"
    FILE_NAME = "testing.csv"

    ## read file from sftp
    process_ftp_file_obj = process_ftp_file( d_datalake_config=dict_adls, d_ftp_config= dict_ftp)    
    read_df = process_ftp_file_obj.read_ftp_files(cnopts)

    ## write data to datalake
    process_ftp_file_obj.write_to_datalake(read_df, ADLS_CONTAINER, ADLS_LOCATION, FILE_NAME )
