import boto3
import logging
from datetime import datetime

bucket_name = 'recebimentodedados-aws-mock1'
s3 = boto3.client('s3')
s3_rcs = boto3.resource('s3')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    try:
        latest_file_upload = search_latest_file(bucket_name)
        
        logger.info(f'Último arquivo carregado: {latest_file_upload}')
        edit_file_name(latest_file_upload)
        
        return {
            'statusCode': 200,
            'body': latest_file_upload
        }
    except FileNotFoundError:
        logger.error('O bucket especificado não foi encontrado.')
        return {
            'statusCode': 404,
            'body': 'O bucket especificado não foi encontrado.'
        }
    except KeyError:
        logger.error('Não há arquivos no bucket especificado.')
        return {
            'statusCode': 404,
            'body': 'Não há arquivos no bucket especificado.'
        }
    except Exception as e:
        logger.error(f'Erro inesperado: {e}')
        return {
            'statusCode': 500,
            'body': str(e)
        }

def search_latest_file(bucket_name: str):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='')

        if 'Contents' not in response:
            raise KeyError("Não há arquivos no bucket especificado.")

        root_objects = [obj for obj in response['Contents'] if '/' not in obj['Key']]

        if not root_objects:
            raise KeyError("Não há arquivos na raiz do bucket especificado.")

        last_modified_object = max(root_objects, key=lambda obj: obj['LastModified'])
        latest_file_upload = last_modified_object['Key']
        return latest_file_upload
    except KeyError as e:
        raise e
    except Exception as e:
        raise e

def edit_file_name(Object_name:str):
    now = datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H:%M:%S")
    logger.info(f'Renomeando arquivo {Object_name} para {Object_name+date_time}')
    s3_rcs.Object(bucket_name, f"processados/{Object_name+date_time}").copy_from(CopySource=f'{bucket_name}/{Object_name}')
    s3_rcs.Object(bucket_name, Object_name).delete()

