import boto3
from botocore.client import Config

# Configuración para conectar con el MinIO que corre en Docker
s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='admin',
                    aws_secret_access_key='password123',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

try:
    s3.create_bucket(Bucket='bronze')
    print("✅ Bucket 'bronze' creado exitosamente en MinIO.")
except Exception as e:
    print(f"⚠️ Nota: {e}")