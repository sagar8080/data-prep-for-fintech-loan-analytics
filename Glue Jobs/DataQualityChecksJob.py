import boto3
from botocore.exceptions import ClientError
import psycopg2
import pandas as pd
import json
import great_expectations as gx
from datetime import datetime
from great_expectations.core.batch import RuntimeBatchRequest

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

# Fetch validation rules for a dataset
def fetch_validation_rules(cursor, dataset_name):
    cursor.execute(
        f"""
        SELECT c.column_name, c.validation_rules
        FROM {RDS_SCHEMA}.datasets d
        JOIN {RDS_SCHEMA}.columns c ON d.dataset_id = c.dataset_id
        WHERE d.dataset_name = %s;
        """,
        (dataset_name,)
    )
    return cursor.fetchall()

# Fetch the dataset from S3
def fetch_dataset_from_s3(s3_key):
    path = f"s3://{S3_BUCKET}/{s3_key}"
    data = pd.read_csv(path)
    return data

def validate_dataset(dataset_name, dataset, context):
    runtime_batch_request = RuntimeBatchRequest(
        datasource_name="my_data",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=dataset_name,  
        runtime_parameters={"batch_data": dataset},  
        batch_identifiers={"default_identifier_name": "default"} 
    )

    # Create a Validator object using the RuntimeBatchRequest
    validator = context.get_validator(batch_request=runtime_batch_request, expectation_suite_name=dataset_name)

    # Run validation
    results = validator.validate()
    return results

def save_validation_results_to_s3(results, dataset_name):
    s3 = boto3.client("s3")
    result_key = f"validation_results/{dataset_name}_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=GX_BUCKET,
        Key=result_key,
        Body=json.dumps(results.to_json_dict(), indent=4),
        ContentType="application/json"
    )
    print(f"Validation results saved to s3://{GX_BUCKET}/{result_key}")


def main():
    # Dataset configuration
    datasets = {
        "state_with_region": "active-processing/state_region.csv",
        "loan_count_yearwise": "active-processing/loan_count_yearwise.csv",
        "loan_purposes": "active-processing/loan_purposes.csv",
        "loan_with_region": "active-processing/loan_with_region.csv",
        "customers": "active-processing/customers.csv",
        "loan_data": "active-processing/loan_data.csv"
    }

    # Connect to RDS
    conn = connect_rds()
    cursor = conn.cursor()

    # Initialize Great Expectations context
    context = gx.data_context.BaseDataContext(
        project_config={
            "config_version": 3,
            "datasources": {
                "my_data": {
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "PandasExecutionEngine"
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["default_identifier_name"]
                        }
                    }
                }
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": GX_BUCKET,
                        "prefix": "expectations/"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": GX_BUCKET,
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
                        "bucket": GX_BUCKET,
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

    # Validate each dataset
    for dataset_name, s3_key in datasets.items():
        print(f"Validating dataset: {dataset_name}")

        # Fetch dataset
        dataset = fetch_dataset_from_s3(s3_key)

        # Fetch validation rules
        validation_rules = fetch_validation_rules(cursor, dataset_name)
        print(f"Validation rules for {dataset_name}: {validation_rules}")

        # Run validation
        results = validate_dataset(dataset_name, dataset, context)

        # Save results
        save_validation_results_to_s3(results, dataset_name)

    # Close RDS connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
