### IMPORT NECESSARY LIBRARIES
import pandas as pd
import datetime
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

load_dotenv(override=True)

def get_container_client():
    """
    Load Azure credentials from .env and return the container client.

    Returns:
        azure.storage.blob.ContainerClient: The container client to interact with Azure Blob Storage.
    """
    connect_str = os.getenv('CONNECT_STR')
    container_name = os.getenv('CONTAINER_NAME')

    if not connect_str or not container_name:
        raise ValueError("Missing Azure connection string or container name in environment variables.")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client

def upload_df_to_blob_as_parquet(df: pd.DataFrame, container_client, blob_name: str) -> None:
    """
    Upload a DataFrame as a Parquet file to Azure Blob Storage.

    Parameters:
        df (pd.DataFrame): The DataFrame to upload.
        container_client: The container client object.
        blob_name (str): Path and name of the blob in Azure Blob Storage.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f" '{blob_name}' uploaded to Azure Blob Storage.")

def data_loading(customer_df: pd.DataFrame, product_df: pd.DataFrame, transaction_df: pd.DataFrame) -> None:
    """
    Handles the full upload process for all tables to Azure Blob Storage.

    Parameters:
        customer_df (pd.DataFrame): Customer table.
        product_df (pd.DataFrame): Product table.
        transaction_df (pd.DataFrame): Transaction fact table.
    """
    container_client = get_container_client()
    upload_df_to_blob_as_parquet(customer_df, container_client, 'rawdata/Customer_table.parquet')
    upload_df_to_blob_as_parquet(product_df, container_client, 'rawdata/Product_table.parquet')
    upload_df_to_blob_as_parquet(transaction_df, container_client, 'rawdata/Transaction_Fact_Table.parquet')
