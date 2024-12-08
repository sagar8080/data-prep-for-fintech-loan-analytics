import boto3
from botocore.exceptions import ClientError
import json
import numpy as np
import datetime
import pandas as pd
import psycopg2
import great_expectations as gx
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError


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

# Read the CSV files from S3
def read_file(file_path):
    path = f"s3://{S3_BUCKET}/{file_path}"
    return pd.read_csv(path)

# Connect to RDS
def connect_rds():
    return psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB
    )

# Fetch metadata for a dataset
def get_dataset_metadata(cursor, dataset_name):
    cursor.execute(
        f"""
        SELECT c.column_name, c.data_type, c.nullable, c.uniqueness
        FROM {RDS_SCHEMA}.datasets d
        JOIN {RDS_SCHEMA}.columns c ON d.dataset_id = c.dataset_id
        WHERE d.dataset_name = %s;
        """,
        (dataset_name,)
    )
    return cursor.fetchall()

# Generate validation rules
def generate_validation_rules(data, column_name, column_metadata):
    col_data = data[column_name]
    rules = []

    if not col_data.isnull().any():
        rules.append({"rule": "expect_column_values_to_not_be_null"})

    if column_metadata.get("unique", False) or col_data.is_unique:
        rules.append({"rule": "expect_column_values_to_be_unique"})

    if pd.api.types.is_numeric_dtype(col_data):
        rules.append({
            "rule": "expect_column_values_to_be_between",
            "min": col_data.min(),
            "max": col_data.max()
        })

    if pd.api.types.is_string_dtype(col_data):
        rules.append({
            "rule": "expect_column_values_to_match_regex",
            "regex": "^[A-Za-z0-9_\\s]*$"
        })

    return rules

# GX expectations suite
def get_or_create_expectation_suite(context, suite_name):
    try:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
        print(f"Loaded existing Expectation Suite: {suite_name}")
    except gx.exceptions.DataContextError:
        suite = context.create_expectation_suite(expectation_suite_name=suite_name)
        print(f"Created new Expectation Suite: {suite_name}")
    return suite

def convert_to_serializable(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return [convert_to_serializable(item) for item in obj]
    elif obj is None:
        return None
    raise TypeError(f"Type {type(obj)} not serializable")


def initialize_context(bucket_name):
    return gx.data_context.BaseDataContext(
        project_config={
            "config_version": 3,
            "datasources": {
                "my_data": {
                    "class_name": "PandasDatasource"
                }
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": bucket_name,
                        "prefix": "expectations/"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": bucket_name,
                        "prefix": "validations/"
                    }
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                }
            },
            "data_docs_sites": {
                "s3_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": bucket_name,
                        "prefix": "data_docs/"
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder"
                    }
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "data_docs_sites_names": ["s3_site"]
        }
    )


def add_column_rules_to_suite(suite, column_name, column_rules):
    for rule in column_rules:
        try:
            if rule["rule"] == "expect_column_values_to_be_between":
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type=rule["rule"],
                        kwargs={"column": column_name, "min_value": rule["min"], "max_value": rule["max"]}
                    )
                )
            elif rule["rule"] == "expect_column_values_to_match_regex":
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type=rule["rule"],
                        kwargs={"column": column_name, "regex": rule["regex"]}
                    )
                )
            else:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type=rule["rule"],
                        kwargs={"column": column_name}
                    )
                )
        except InvalidExpectationConfigurationError as e:
            print(f"Invalid expectation for column '{column_name}': {str(e)}. Skipping this rule.")


def update_validation_rules_in_rds(cursor, column_rules, column_name, dataset_name, schema):
    column_rules_serializable = json.dumps(column_rules, default=convert_to_serializable)
    cursor.execute(
        f"""
        UPDATE {schema}.columns
        SET validation_rules = %s
        WHERE column_name = %s AND (SELECT dataset_id FROM {schema}.datasets WHERE dataset_name = %s) = dataset_id;
        """,
        (column_rules_serializable, column_name, dataset_name)
    )


def process_columns(data, column_metadata, suite, cursor, dataset_name, schema):
    for col in column_metadata:
        column_name = col[0]
        column_rules = generate_validation_rules(data, column_name, {
            "data_type": col[1],
            "nullable": col[2],
            "uniqueness": col[3]
        })

        print(f"Adding rules for column: {column_name}")
        add_column_rules_to_suite(suite, column_name, column_rules)
        update_validation_rules_in_rds(cursor, column_rules, column_name, dataset_name, schema)


def profile_dataset(dataset_name, s3_key, conn):
    data = read_file(s3_key)

    # Fetch metadata from RDS
    cursor = conn.cursor()
    column_metadata = get_dataset_metadata(cursor, dataset_name)

    # Initialize Great Expectations context
    context = initialize_context(GX_BUCKET)

    # Create or fetch the expectation suite
    suite = get_or_create_expectation_suite(context, suite_name=dataset_name)

    # Process columns and update validation rules
    process_columns(data, column_metadata, suite, cursor, dataset_name, RDS_SCHEMA)

    # Save the expectation suite and build data docs
    context.save_expectation_suite(suite)
    context.build_data_docs()

    # Commit changes and close the cursor
    conn.commit()
    cursor.close()

def main():
    datasets = {
        "state_with_region": "active-processing/state_region.csv",
        "loan_count_yearwise": "active-processing/loan_count_yearwise.csv",
        "loan_purposes": "active-processing/loan_purposes.csv",
        "loan_with_region": "active-processing/loan_with_region.csv",
        "customers": "active-processing/customers.csv",
        "loan_data": "active-processing/loan_data.csv"
    }
    conn = connect_rds()
    for dataset_name, s3_key in datasets.items():
        profile_dataset(dataset_name, s3_key, conn)
    conn.close()


if __name__ == "__main__":
    main()