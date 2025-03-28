import argparse
import pandas as pd
from sqlalchemy import create_engine
import json
import os

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process and clean datasets, then upload to PostgreSQL.")
    parser.add_argument("--source_dir", type=str, required=True, help="Directory containing CSV and Parquet files.")
    parser.add_argument("--postgres_config", type=str, required=True, help="Path to PostgreSQL configuration JSON file.")
    parser.add_argument("--table_name", type=str, required=True, help="Name of the table to store data in PostgreSQL.")
    return parser.parse_args()

def read_and_combine_datasets(source_dir):
    dataframes = []
    for filename in os.listdir(source_dir):
        file_path = os.path.join(source_dir, filename)
        if os.path.isfile(file_path):
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
                dataframes.append(df)
            elif filename.lower().endswith('.parquet'):
                df = pd.read_parquet(file_path)
                dataframes.append(df)
    if not dataframes:
        raise ValueError("No CSV or Parquet files found in the directory.")
    return pd.concat(dataframes, ignore_index=True)

def clean_data(df):
    # Convert all column names to lowercase
    df.columns = df.columns.str.lower()

    escape_sequence_pattern = r"\\x[a-fA-F0-9]{2}|\\\\|\\n+"
    for col in df.select_dtypes(include="object").columns:
        # Replace escape sequences, strip spaces, and convert to lowercase
        df[col] = df[col].astype(str).replace(escape_sequence_pattern, "", regex=True).str.strip().str.lower()
        # Replace NaN with an empty string (for string columns)
        df[col] = df[col].fillna('')

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce").dt.date

    if "amount_in_usd" in df.columns:
        df["amount_in_usd"] = pd.to_numeric(df["amount_in_usd"].str.replace(",", "", regex=True), errors="coerce").fillna(0)

    return df

def connect_to_postgres(config_file):
    with open(config_file) as f:
        config = json.load(f)
    connection_string = (
        f"postgresql+pg8000://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )
    return create_engine(connection_string)

def main():
    args = parse_arguments()
    combined_df = read_and_combine_datasets(args.source_dir)
    cleaned_df = clean_data(combined_df)
    engine = connect_to_postgres(args.postgres_config)
    cleaned_df.to_sql(args.table_name, con=engine, if_exists="replace", index=False)
    print("Data processing complete.")

if __name__ == "__main__":
    main()







##(base) user@user-Latitude-3420:~/TRACK1$ python /home/user/ETL/1.py --source_dir /home/user/ETL/source_dir --postgres_config /home/user/ETL/config.json --table_name combine