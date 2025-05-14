import pandas as pd
import logging
from Extract import load_csv_data
from Transform import transform_data
from load import create_tables
from helpers import get_container_client, data_loading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting ETL pipeline...")

    try:
        # Extract: Load CSV data
        logger.info("Extracting data from CSV...")
        ziko_df = load_csv_data('ziko_logistics_data.csv')
        logger.info("Data loaded successfully.")

        # Transform: Clean and transform the data
        logger.info("Transforming data...")
        ziko_df = transform_data(ziko_df)
        logger.info("Data transformed successfully.")

        # Create Tables: Prepare dimension and fact tables
        logger.info("Creating dimension and fact tables...")
        customer_df, product_df, transaction_df = create_tables(ziko_df)
        logger.info("Dimension and fact tables created.")

        # Get Azure Blob Storage client
        logger.info("Connecting to Azure Blob Storage...")
        container_client = get_container_client()
        logger.info("Connected to Azure Blob Storage.")

        # Load to Azure Blob Storage
        logger.info("Uploading data to Azure Blob Storage...")
        data_loading(customer_df, product_df, transaction_df, container_client)
        logger.info("Data uploaded to Azure Blob Storage.")

        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)

if __name__ == '__main__':
    main()
