import pandas as pd
import json
import ast
import re
import psycopg2
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

input_datasets = {
    "state_with_region": "post-processing/state_region.csv",
    "loan_count_yearwise": "post-processing/loan_count_yearwise.csv",
    "loan_purposes": "post-processing/loan_purposes.csv",
    "loan_with_region": "post-processing/loan_with_region.csv",
    "customer_data": "post-processing/customers.csv",
    "loan_data": "post-processing/loan_data.csv"
}

# Fetch secret from AWS Secrets Manager
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

# Database connection parameters
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

# Step 1: Loan Approval Indicator
def add_loan_approval_indicator(loan):
    """
    Add a 'loan_approval' column to indicate the loan status.
    """
    loan['loan_approval'] = loan['loan_status'].apply(
        lambda x: 'Approved' if x in ['Fully Paid', 'Current'] else
        ('Defaulted' if x in ['Default', 'Charged Off'] else
         ('Late' if x in ['Late (16-30 days)', 'Late (31-120 days)'] else 'Other'))
    )
    return loan

def calculate_approval_rate(loan, customer):
    """
    Merge loan and customer data, and calculate approval rate by demographics.
    """
    loan_approval_by_demographics = pd.merge(loan, customer, on='customer_id', how='left')
    approval_rate = loan_approval_by_demographics.groupby(
        ['home_ownership', 'employment_length', 'verification_status']
    ).agg({
        'loan_approval': lambda x: (x == 'Approved').mean(),
        'loan_amount': 'mean'
    }).reset_index()

    approval_rate['Loan Approval Rate'] = (approval_rate['loan_approval'] * 100).round(0)
    approval_rate['Loan Approval Rate'] = approval_rate['Loan Approval Rate'].apply(lambda x: f"{int(x)}%")
    approval_rate['Average Loan Amount (USD)'] = approval_rate['loan_amount'].apply(
        lambda x: f"${x:,.2f}"
    )
    approval_rate.drop(columns=['loan_approval'], inplace=True)

    return approval_rate

def calculate_regional_trends(loan, loan_with_region, state_region):
    """
    Calculate regional loan trends by merging loan data with region and state data.
    """
    loan_region_data = pd.merge(loan_with_region, state_region, how='left', on='region')
    loan_region_data_full = pd.merge(loan_region_data, loan[['loan_id', 'interest_rate', 'loan_term']], how='left', on='loan_id')

    group_columns = ['region', 'subregion'] if 'subregion' in loan_region_data_full.columns else ['region']

    regional_loan_trends = loan_region_data_full.groupby(group_columns).agg({
        'loan_amount': 'sum',
        'interest_rate': 'mean',
        'loan_term': 'mean'
    }).reset_index()

    # Format results
    regional_loan_trends['loan_amount'] = pd.to_numeric(regional_loan_trends['loan_amount'], errors='coerce')
    regional_loan_trends['loan_amount'] = regional_loan_trends['loan_amount'].apply(lambda x: "{:,.0f}".format(x) if pd.notna(x) else "Unknown")
    regional_loan_trends['interest_rate'] = regional_loan_trends['interest_rate'].apply(lambda x: "{:.2f}%".format(x * 100) if isinstance(x, (float, int)) else "Unknown")
    regional_loan_trends['loan_term'] = regional_loan_trends['loan_term'].apply(lambda x: "{:02d}".format(round(x)) if isinstance(x, (float, int)) else "Unknown")

    return regional_loan_trends

# Loan Purpose Trends
def calculate_loan_purpose_trends(loan, loan_purposes, loan_count_by_year):
    """
    Calculate loan trends by purpose and year.
    """
    loan_with_purpose = pd.merge(loan, loan_purposes, how='left', on='purpose')
    loan_purpose_trends = loan_with_purpose.groupby(['purpose', 'issue_year']).size().reset_index(name='loan_count_by_purpose')
    loan_purpose_trends = pd.merge(loan_purpose_trends, loan_count_by_year, on='issue_year', how='left')

    loan_purpose_trends.rename(columns={
        'loan_count_x': 'loan_count_by_purpose',
        'loan_count_y': 'total_loan_count_by_year'
    }, inplace=True)

    return loan_purpose_trends

# Customer Risk and Returns
def calculate_customer_risk_and_returns(loan, customer):
    """
    Calculate return and risk for customer segments.
    """
    loan['return'] = loan['loan_amount'] * (1 + loan['interest_rate'])

    # Risk mapping
    risk_mapping = {
        'Fully Paid': 'Low',
        'Current': 'Low',
        'Late (16-30 days)': 'High',
        'Late (31-120 days)': 'High',
        'Default': 'High',
        'Charged Off': 'High'
    }
    loan['risk'] = loan['loan_status'].map(risk_mapping)

    customer_loan_performance = pd.merge(loan, customer, on='customer_id')

    performance_by_segment = customer_loan_performance.groupby(['home_ownership', 'verification_status']).agg({
        'return': 'mean',
        'risk': lambda x: (x == 'High').mean()
    }).reset_index()

    performance_by_segment.rename(columns={
        'return': 'Average Return (USD)',
        'risk': 'Default Rate (%)'
    }, inplace=True)

    performance_by_segment['Average Return (USD)'] = performance_by_segment['Average Return (USD)'].apply(lambda x: f"{x:,.2f}")
    performance_by_segment['Default Rate (%)'] = performance_by_segment['Default Rate (%)'].apply(lambda x: f"{x*100:.2f}%")

    return performance_by_segment

# Save Transformed Data
def save_transformed_data(dfs, file_names):
    """
    Save transformed data to CSV files for visualization.
    """
    for df, file_name in zip(dfs, file_names):
        df.to_csv(file_name, index=False)
        print(f"Saved transformed data to {file_name}")

def save_to_db(df, table_name, engine, schema="public"):
    """
    Save DataFrame to the specified database table.
    """
    try:
        with engine.connect() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                schema=schema,
                if_exists="replace",  # Replace the table if it already exists
                index=False  # Do not write DataFrame index as a column
            )
        print(f"Table '{table_name}' saved to database successfully.")
    except Exception as e:
        print(f"Error saving table '{table_name}' to database: {e}")

def generate_ddl(df, table_name, schema="loans"):
    """
    Generate a SQL DDL statement based on the DataFrame's schema.
    """
    ddl = f"CREATE TABLE {schema}.{table_name} (\n"
    for column, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            sql_type = "INT"
        elif "float" in str(dtype):
            sql_type = "FLOAT"
        elif "object" in str(dtype):
            sql_type = "VARCHAR(255)"
        elif "datetime" in str(dtype):
            sql_type = "TIMESTAMP"
        else:
            sql_type = "TEXT"
        ddl += f"    {column} {sql_type},\n"
    ddl = ddl.rstrip(",\n") + "\n);"
    return ddl

def main_data_transformations():
    # Load Data
    try:
        # Load required datasets
        customer_data = read_file(input_datasets['customer_data'])
        loan_data = read_file(input_datasets['loan_data'])
        loan_count_by_year = read_file(input_datasets['loan_count_yearwise'])
        loan_purpose = read_file(input_datasets['loan_purposes'])
        loan_with_region = read_file(input_datasets['loan_with_region'])
        state_region = read_file(input_datasets['state_with_region'])
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Add Loan Approval Indicator
    try:
        loan = add_loan_approval_indicator(loan_data)
        print("Loan approval indicator added.")
    except Exception as e:
        print(f"Error adding loan approval indicator: {e}")
        return

    # Calculate Approval Rates
    try:
        approval_rate = calculate_approval_rate(loan, customer_data)
        print("Approval rates calculated.")
    except Exception as e:
        print(f"Error calculating approval rates: {e}")
        return

    # Calculate Regional Loan Trends
    try:
        regional_loan_trends = calculate_regional_trends(loan, loan_with_region, state_region)
        print("Regional loan trends calculated.")
    except Exception as e:
        print(f"Error calculating regional loan trends: {e}")
        return

    # Calculate Loan Purpose Trends
    try:
        loan_purpose_trends = calculate_loan_purpose_trends(loan, loan_purpose, loan_count_by_year)
        print("Loan purpose trends calculated.")
    except Exception as e:
        print(f"Error calculating loan purpose trends: {e}")
        return

    # Calculate Customer Risk and Returns
    try:
        performance_by_segment = calculate_customer_risk_and_returns(loan, customer_data)
        print("Customer risk and returns calculated.")
    except Exception as e:
        print(f"Error calculating customer risk and returns: {e}")
        return

    # Save Transformed Data to Database
    try:
        save_to_db(approval_rate, "approval_rate", engine, schema="loans")
        save_to_db(regional_loan_trends, "regional_loan_trends", engine, schema="loans")
        save_to_db(loan_purpose_trends, "loan_purpose_trends", engine, schema="loans")
        save_to_db(performance_by_segment, "performance_by_segment", engine, schema="loans")
        print("Transformed data saved to database successfully.")
    except Exception as e:
        print(f"Error saving transformed data to database: {e}")
        return

if __name__ == "__main__":
    main_data_transformations()