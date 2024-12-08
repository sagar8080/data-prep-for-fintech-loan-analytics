import pandas as pd
import json
import ast
import re
import psycopg2
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

input_datasets = {
    "state_with_region": "active-processing/state_region.csv",
    "loan_count_yearwise": "active-processing/loan_count_yearwise.csv",
    "loan_purposes": "active-processing/loan_purposes.csv",
    "loan_with_region": "active-processing/loan_with_region.csv",
    "customers": "active-processing/customers.csv",
    "loan_data": "active-processing/loan_data.csv"
}

output_datasets = {
    "state_with_region": "post-processing/state_region.csv",
    "loan_count_yearwise": "post-processing/loan_count_yearwise.csv",
    "loan_purposes": "post-processing/loan_purposes.csv",
    "loan_with_region": "post-processing/loan_with_region.csv",
    "customer_data": "post-processing/customers.csv",
    "loan_data": "post-processing/loan_data.csv"
}

def get_secret():
    secret_name = "db-secret"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except ClientError as e:
        print(f"Error Occurred: {e}")
        return None

secret = get_secret()
RDS_HOST = secret["host"]
RDS_PORT = int(secret["port"])
RDS_USER = secret["username"]
RDS_PASSWORD = secret["password"]
RDS_DB = "fintech"
RDS_SCHEMA = "loans"
S3_BUCKET = "source-system-754"
GX_BUCKET = "project-utility-754"

CONNECTION_STRING = f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}"
try:
    engine = create_engine(CONNECTION_STRING)
    connection = engine.connect()
    print("Connection successful!")
    connection.close()
except Exception as e:
    print(f"Connection failed: {e}")

def read_file(file_path):
    path = f"s3://{S3_BUCKET}/{file_path}"
    data = pd.read_csv(path)
    return data

def write_file(df, file_path):
    path = f"s3://{S3_BUCKET}/{file_path}"
    df.to_csv(path, index=False)


def decode_ibm866(byte_values):
    """Decode IBM866 byte values."""
    try:
        return byte_values.decode("ibm866", errors="ignore")
    except Exception as e:
        print(f"Error decoding bytes: {e}")
        return None

def convert_to_bytes(encoded_str):
    """Convert encoded string to byte values."""
    try:
        return ast.literal_eval(encoded_str)
    except Exception as e:
        print(f"Error converting string to bytes: {e}")
        return None
    
def clean_customer_id(customer_id):
    """Clean customer ID by removing non-ASCII and special characters."""
    # Ensure the string is in UTF-8 format
    customer_id = customer_id.encode("utf-8").decode("utf-8", errors="ignore")
    # Remove non-ASCII characters
    customer_id = re.sub(r"[^\x00-\x7F]+", "", customer_id)
    # Remove special characters except for alphanumeric, '-', and '_'
    customer_id = re.sub(r"[^a-zA-Z0-9-_]", "", customer_id)
    return customer_id

# Step 2: Processing Functions
def decode_and_clean_customer_ids(df):
    """Apply decoding and cleaning transformations to the 'customer_id' column."""
    df["customer_id"] = df["customer_id"].apply(convert_to_bytes)
    df["customer_id"] = df["customer_id"].apply(lambda x: decode_ibm866(x))
    df["customer_id"] = df["customer_id"].apply(clean_customer_id)
    df["customer_id"] = df["customer_id"].str.replace(r"[^\x00-\x7F]+", "", regex=True)
    df["customer_id"] = df["customer_id"].str.replace("�", "", regex=False)
    df["customer_id"] = df["customer_id"].str.replace("-", "", regex=False)
    df["customer_id"] = df["customer_id"].str.replace("_", "", regex=False)
    return df

def clean_data(data):
    if not isinstance(data, pd.DataFrame):
        df = pd.DataFrame(data)
    else:
        df = data
    df = decode_and_clean_customer_ids(df)
    return df


def handle_missing_values(customer, loan):
    customer.dropna(subset=["customer_id"], inplace=True)
    loan.dropna(subset=["loan_id", "customer_id"], inplace=True)
    customer["emp_length"] = customer["emp_length"].fillna("Unknown")
    return customer, loan


def replace_na_values(datasets):
    for df in datasets:
        df.replace(["n/a", ""], pd.NA, inplace=True)
    return datasets

def normalize_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def rename_columns(customer, loan, state_region):
    """Rename columns for consistency across datasets."""
    customer.rename(
        columns={
            "emp_length": "employment_length",
            "avg_cur_bal": "average_current_balance",
            "tot_cur_bal": "total_current_balance",
            "addr_state": "state",
            "emp_title": "employee_title",
            "annual_inc": "annual_income",
            "annual_inc_joint": "annual_joint_income",
        },
        inplace=True,
    )

    loan.rename(
        columns={
            "term": "loan_term",
            "int_rate": "interest_rate",
            "pymnt_plan": "payment_plan",
        },
        inplace=True,
    )

    state_region.rename(columns={"subregion": "sub_region"}, inplace=True)

    return customer, loan, state_region

def clean_loan_data(loan):
    loan["loan_term"] = loan["loan_term"].str.extract(r"(\d+)").astype(float)

    # Convert interest_rate to numeric, handling errors
    loan["interest_rate"] = pd.to_numeric(loan["interest_rate"], errors="coerce")

    # Replace NaN values in interest_rate and loan_term with 'Unknown'
    loan["interest_rate"] = loan["interest_rate"].fillna("Unknown")
    loan["loan_term"] = loan["loan_term"].fillna("Unknown")

    return loan

# Drop Unnecessary Columns
def drop_unnecessary_columns(loan):
    """Drop specified columns from the loan dataset."""
    columns_to_drop = ["notes", "issue_d"]
    loan.drop(columns=columns_to_drop, inplace=True, errors="ignore")
    return loan

# Convert Columns to Numeric
def convert_to_numeric(customer, loan):
    customer["annual_income"] = pd.to_numeric(customer["annual_income"], errors="coerce")
    loan["interest_rate"] = pd.to_numeric(loan["interest_rate"], errors="coerce")
    return customer, loan

def save_to_db(df, table_name, schema="loans"):
    try:
        with engine.connect() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                schema=schema,
                if_exists="replace",
                index=False
            )
        print(f"Table '{table_name}' saved to database successfully.")
        write_file(df, output_datasets[table_name])
    except Exception as e:
        print(f"Error saving DataFrame: {e}")


def main():
    try:
        # Load original files for auxiliary datasets
        customer_data = read_file(input_datasets['customers'])
        loan_data = read_file(input_datasets['loan_data'])
        loan_count_by_year = read_file(input_datasets['loan_count_yearwise'])
        loan_purpose = read_file(input_datasets['loan_purposes'])
        loan_with_region = read_file(input_datasets['loan_with_region'])
        state_region = read_file(input_datasets['state_with_region'])
        decoded_customer_data = clean_data(customer_data)
        decoded_loan_data = clean_data(loan_data)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    try:
        customer_data, loan_data = handle_missing_values(decoded_customer_data, decoded_loan_data)
        print("Missing values handled.")
    except Exception as e:
        print(f"Error handling missing values: {e}")
        return

    try:
        datasets = [customer_data, loan_data, loan_count_by_year, loan_purpose, loan_with_region, state_region]
        datasets = replace_na_values(datasets)
        customer_data, loan_data, loan_count_by_year, loan_purpose, loan_with_region, state_region = datasets
        print("Replaced 'n/a' and empty strings with NaN.")
    except Exception as e:
        print(f"Error replacing 'n/a' values: {e}")
        return

    try:
        customer_data = normalize_columns(customer_data)
        loan_data = normalize_columns(loan_data)
        loan_count_by_year = normalize_columns(loan_count_by_year)
        loan_purpose = normalize_columns(loan_purpose)
        loan_with_region = normalize_columns(loan_with_region)
        state_region = normalize_columns(state_region)
        print("Column names normalized.")
    except Exception as e:
        print(f"Error normalizing column names: {e}")
        return

    try:
        customer_data, loan_data, state_region = rename_columns(customer_data, loan_data, state_region)
        print("Columns renamed for consistency.")
    except Exception as e:
        print(f"Error renaming columns: {e}")
        return

    try:
        loan_data = clean_loan_data(loan_data)
        print("Loan data cleaned.")
    except Exception as e:
        print(f"Error cleaning loan data: {e}")
        return

    try:
        loan_data = drop_unnecessary_columns(loan_data)
        print("Unnecessary columns dropped.")
    except Exception as e:
        print(f"Error dropping unnecessary columns: {e}")
        return

    try:
        customer_data, loan_data = convert_to_numeric(customer_data, loan_data)
        print("Columns converted to numeric.")
    except Exception as e:
        print(f"Error converting columns to numeric: {e}")
        return
    
    try:
        save_to_db(customer_data, "customer_data", schema="loans")
        save_to_db(loan_data, "loan_data", schema="loans")
        save_to_db(loan_with_region, "loan_with_region", schema="loans")
        save_to_db(loan_purpose, "loan_purposes", schema="loans")
        save_to_db(state_region, "state_with_region", schema="loans")
        save_to_db(loan_count_by_year, "loan_count_yearwise", schema="loans")
        print("Cleaned data saved to database.")
    except Exception as e:
        print(f"Error saving data to database: {e}")
        return

    print("Data processing completed successfully.")


if __name__ == "__main__":
    main()
