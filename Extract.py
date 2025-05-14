import pandas as pd
import os

def load_csv_data(file_path: str) -> pd.DataFrame:
    """
    Load CSV data from the given file path.

    Parameters:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.

    Raises:
        FileNotFoundError: If the file does not exist.
        pd.errors.EmptyDataError: If the file is empty.
        pd.errors.ParserError: If there's an error parsing the file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} was not found.")
    
    try:
        df = pd.read_csv(file_path)
        return df
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty.")
    except pd.errors.ParserError as e:
        raise ValueError(f"Parsing error: {e}")

# Example usage
ziko_df = load_csv_data('ziko_logistics_data.csv')
