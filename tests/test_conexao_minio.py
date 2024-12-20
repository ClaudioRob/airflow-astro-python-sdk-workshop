import boto3
import json

s3_client = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="LtTnhoirCZie2UTICHGj",
    aws_secret_access_key="oGvKii91jipCrk4cXQx3SzIoulUBabOc26Az5P7S",
)

# Listar os buckets no MinIO
response = s3_client.list_buckets()

# Formatar a resposta como JSON
formatted_response = json.dumps(response, indent=4, default=str)

# Exibir a resposta formatada
print(formatted_response)
# print(response)