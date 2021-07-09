"""
Utilities for interacting with AWS
"""
import boto3
import botocore
import sys
import json

ENDPOINT_URL = "https://secretsmanager.us-east-1.amazonaws.com"
REGION_NAME = "us-east-1"

SESSION = boto3.session.Session()
SECRETS_CLIENT = SESSION.client(
    service_name='secretsmanager', region_name=REGION_NAME, endpoint_url=ENDPOINT_URL)


def get_queue_by_name(name):
    """
    Returns the SQS queue associated with the name provided
    """
    sqs = boto3.resource('sqs')
    return sqs.get_queue_by_name(QueueName=name)


def get_s3_client():
    """
    Returns an S3 client
    """
    return boto3.resource('s3')


def get_secret_from_secretes_manager(secret_id):
    """
    Returns the secret string from Secrets Manager

    If unable to retrieve the secret it kills the running process
    """
    try:
        secret_response = SECRETS_CLIENT.get_secret_value(SecretId=secret_id)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_id + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        print('Fatal error. Stopping program.')
        sys.exit()
    else:
        if 'SecretString' in secret_response:
            return json.loads(secret_response['SecretString'])
        elif 'SecretBinary' in secret_response:
            return secret_response['SecretBinary']
        else:
            print('Fatal error. Unexpected secret type.')
            sys.exit()


def write_aspera_secrets_to_disk():
    """
    Fetches Aspera secrets from Secrets Manager and writes them to disk
    """
    aspera_key_secret = get_secret_from_secretes_manager('ups-prod-aspera-key')
    aspera_file = open("/aspera/aspera.pk", "wb")
    aspera_file.write(aspera_key_secret)
    aspera_file.flush()
    aspera_file.close()

    aspera_vcf_key_secret = get_secret_from_secretes_manager('ups-prod-aspera-vcf-key')
    aspera_vcf_file = open("/aspera/aspera_vcf.pk", "wb")
    aspera_vcf_file.write(aspera_vcf_key_secret)
    aspera_vcf_file.flush()
    aspera_vcf_file.close()
