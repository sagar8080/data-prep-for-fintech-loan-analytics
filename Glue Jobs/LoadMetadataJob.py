import sys
import boto3
import pandas as pd
import psycopg2
import json
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


DEFAULT_PATH_MAP = {
    "state_with_region": "active-processing/state_region.csv",
    "loan_count_yearwise": "active-processing/loan_count_yearwise.csv",
    "loan_purposes": "active-processing/loan_purposes.csv",
    "loan_with_region": "active-processing/loan_with_region.csv",
    "customers": "active-processing/customers.csv",
    "loan_data": "active-processing/loan_data.csv"
}
RDS_DB = "fintech"
RDS_SCHEMA = "loans"
S3_BUCKET = "source-system-754"

def get_secret():
    secret_name = "db-secret"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error Occured: {e}")
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    return secret

def map_dtype_to_sql(pandas_dtype):
    mapping = {
        "int64": "INTEGER",
        "float64": "FLOAT",
        "object": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP"
    }
    return mapping.get(pandas_dtype, "TEXT")

def read_file(file_path):
    path = f"s3://{S3_BUCKET}/{file_path}"
    data = pd.read_csv(path)
    return data


secret = get_secret()
RDS_HOST = secret['host']
RDS_PORT = int(secret['port'])
RDS_USER = secret['username']
RDS_PASSWORD = secret['password']
conn = psycopg2.connect(
    host=RDS_HOST,
    port=RDS_PORT,
    user=RDS_USER,
    password=RDS_PASSWORD,
    database=RDS_DB
)
cursor = conn.cursor()
print("Connected to RDS")

def load_metadata(cursor):
    for dataset_name, object_key in DEFAULT_PATH_MAP.items():
        data = read_file(object_key)
        columns_metadata = [
            {
                "column_name": col,
                "data_type": str(data[col].dtypes),
                "nullable": bool(data[col].isnull().any()),
                "uniqueness": bool(data[col].is_unique)
            }
            for col in data.columns
        ]

        cursor.execute(
            f"""
            INSERT INTO {RDS_SCHEMA}.datasets (dataset_name, s3_path)
            VALUES (%s, %s)
            ON CONFLICT (dataset_name) DO NOTHING
            RETURNING dataset_id;
            """,
            (dataset_name, f"s3://{S3_BUCKET}/{object_key}")
        )
        dataset_id_result = cursor.fetchone()
        if dataset_id_result:
            dataset_id = dataset_id_result[0]
        else:
            cursor.execute(
                f"SELECT dataset_id FROM {RDS_SCHEMA}.datasets WHERE dataset_name = %s;",
                (dataset_name,)
            )
            dataset_id = cursor.fetchone()[0]
        for col_meta in columns_metadata:
            cursor.execute(
                f"""
                INSERT INTO {RDS_SCHEMA}.columns 
                (dataset_id, column_name, data_type, nullable, uniqueness, validation_rules)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset_id, column_name) DO UPDATE SET
                    data_type = EXCLUDED.data_type,
                    nullable = EXCLUDED.nullable,
                    uniqueness = EXCLUDED.uniqueness;
                """,
                (
                    dataset_id,
                    col_meta["column_name"],
                    map_dtype_to_sql(col_meta["data_type"]),
                    col_meta["nullable"],
                    col_meta["uniqueness"],
                    None
                )
            )
    print(f"Metadata for dataset '{dataset_name}' successfully loaded into RDS.")

load_metadata()
conn.commit()
cursor.close()
conn.close()