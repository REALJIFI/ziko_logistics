import os
import io
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


def create_tables(df: pd.DataFrame) -> tuple:
    """
    Create dimension and fact tables from the main DataFrame.

    Parameters:
        df (pd.DataFrame): The input cleaned logistics data.

    Returns:
        tuple: A tuple containing:
            - Customer_table (pd.DataFrame)
            - Product_table (pd.DataFrame)
            - Transaction_Fact_Table (pd.DataFrame)
    """
    Customer_table = df[['Customer_ID','Customer_Name','Customer_Phone', 
                         'Customer_Email', 'Customer_Address']].copy().drop_duplicates()\
                        .reset_index(drop=True)

    Product_table = df[['Product_ID','Product_List_Title','Unit_Price',
                        'Quantity']].copy().drop_duplicates()\
                       .reset_index(drop=True)

    Transaction_Fact_Table = df.merge(Customer_table, 
                                      on=['Customer_ID','Customer_Name','Customer_Phone', 
                                          'Customer_Email', 'Customer_Address'], 
                                      how='left')\
                               .merge(Product_table, 
                                      on=['Product_ID','Product_List_Title','Unit_Price','Quantity'], 
                                      how='left')\
                               [['Transaction_ID','Product_ID','Customer_ID','Total_Cost', 
                                 'Discount_Rate', 'Sales_Channel','Order_Priority', 'Warehouse_Code', 
                                 'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 
                                 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 
                                 'Region', 'Country']]

    return Customer_table, Product_table, Transaction_Fact_Table









