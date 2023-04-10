import os

import boto3
import botocore

function_name = "crowemi-trades-lambda-get-data"
role = "arn:aws:iam::926488920335:role/crowemi-trades-lambda-get-data"
image_uri = f"{os.getenv('ECR_REGISTRY')}/{os.getenv('ECR_REPOSITORY')}:{os.getenv('IMAGE_TAG')}"
polygon_key = os.getenv("POLYGON_KEY")

client = boto3.client("lambda")

definition = {
    "FunctionName": function_name,
    "Role": role,
    "Code": {"ImageUri": image_uri},
    "PackageType": "Image",
    "Environment": {"Variables": {"POLYGON_KEY": polygon_key}},
    "Timeout": 900,
}

try:
    response = client.get_function(FunctionName="crowemi-trades-lambda-get-data")
    response = client.update_function_code(
        FunctionName=function_name,
        ImageUri=image_uri,
        Publish=True,
    )
    print(response)
except botocore.exceptions.ClientError as e:
    if e.response["Error"]["Code"] == "ResourceNotFoundException":
        response = client.create_function(**definition)
        print(response)
    else:
        raise e
