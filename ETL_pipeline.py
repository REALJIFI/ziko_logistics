### IMPORT NECESSARY LIBRARIES
import pandas as pd
import datetime
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

### EXTRACTION LAYER
ziko_df = pd.read_csv('ziko_logistics_data.csv')

### Data cleaning and transformation
ziko_df.fillna({
    'Unit_Price': ziko_df['Unit_Price'].mean(), # unit price dtype is float so by default fill it with mean
    'Total_Cost': ziko_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'Unknown'
    }, inplace=True)


ziko_df.info()

# correct and convert Date datatype
ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])

ziko_df.info()

### CREATE TABLE
Customer_table = ziko_df[['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates()\
.reset_index(drop=True)

### CREATE TABLE
Product_table = ziko_df[['Product_ID','Product_List_Title','Unit_Price','Quantity']].copy().drop_duplicates()\
.reset_index(drop=True)

### CREATE TABLE
Transaction_Fact_Table = ziko_df.merge(Customer_table, on= ['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address'], how='left')\
                                .merge(Product_table, on= ['Product_ID','Product_List_Title','Unit_Price','Quantity'], how='left')\
                                [['Transaction_ID','Product_ID','Customer_ID','Total_Cost', 'Discount_Rate', 'Sales_Channel','Order_Priority', 'Warehouse_Code', 'Ship_Mode', 'Delivery_Status',\
                                'Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country',]]

### LOAD TEMPORALLY AS CSV
Customer_table.to_csv(r'Dataset\Customer_table.csv', index=False)
Product_table.to_csv(r'Dataset\Product_table.csv', index=False)
Transaction_Fact_Table.to_csv(r'Dataset\Transaction_Fact_Table.csv', index=False)

print('file have been loaded temporarily into local machine')

### DATA LOADING
# set up azure blob connection
load_dotenv()
connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)

### Create a function that will load the data into azure blob storage as parquet file
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to blob storage successfully')

## data upload
upload_df_to_blob_as_parquet(Customer_table, container_client, 'rawdata/Customer_table.parquet')
upload_df_to_blob_as_parquet(Product_table, container_client, 'rawdata/Product_table.parquet')
upload_df_to_blob_as_parquet(Transaction_Fact_Table, container_client, 'rawdata/Transaction_Fact_Table.parquet')