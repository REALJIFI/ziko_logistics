import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the input DataFrame.

    Operations:
    - Fill missing values:
        - 'Unit_Price' and 'Total_Cost' with their respective means.
        - 'Discount_Rate' with 0.0.
        - 'Return_Reason' with 'Unknown'.
    - Convert 'Date' column to datetime.

    Parameters:
        df (pd.DataFrame): The input DataFrame to transform.

    Returns:
        pd.DataFrame: The cleaned and transformed DataFrame.
    """
    df.fillna({
        'Unit_Price': df['Unit_Price'].mean(),  
        'Total_Cost': df['Total_Cost'].mean(),
        'Discount_Rate': 0.0,
        'Return_Reason': 'Unknown'
    }, inplace=True)

    # Convert 'Date' column to datetime, coerce errors if any invalid entries
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    return df


