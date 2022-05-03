import  logging
_logger: logging.Logger = logging.getLogger(__name__)
from datapipeline import config
import boto3
def boto3_session():
    region_name:str=None
    aws_access_key_id:str=None
    aws_secret_access_key:str=None
    boto3_session = None
    try:

        region_name=config.get_config('S3','REGION_NAME')
        aws_access_key_id=config.get_config('S3','AWS_ACCESS_KEY')
        aws_secret_access_key=config.get_config('S3','AWS_SECRET_KEY')
        boto3_session = boto3.Session(region_name=region_name,
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key)
    except Exception as e:
        _logger.warning(f's3 credential not defined Using  aws configure setting')
        boto3_session = boto3.Session(region_name=region_name)

    return boto3_session
