import os

import boto3
import botocore

function_name = "crowemi-trades-lambda-get-data"
role = "arn:aws:iam::926488920335:role/crowemi-trades-lambda-get-data"
image_uri = f"{os.getenv('ECR_REGISTRY', '926488920335.dkr.ecr.us-west-2.amazonaws.com')}/{os.getenv('ECR_REPOSITORY', 'crowemi-trades-lambda-get-data')}:{os.getenv('IMAGE_TAG', 'latest')}"
polygon_key = os.getenv("POLYGON_KEY")
mongodb_uri = os.getenv("MONGODB_URI")

client = boto3.client("lambda")

definition = {
    "FunctionName": function_name,
    "Role": role,
    "Code": {"ImageUri": image_uri},
    "PackageType": "Image",
    "Environment": {
        "Variables": {"POLYGON_KEY": polygon_key, "MONGODB_URI": mongodb_uri}
    },
    "Timeout": 900,
}

try:
    response = client.get_function(FunctionName="crowemi-trades-lambda-get-data")
    # update code
    response = client.update_function_code(
        FunctionName=function_name,
        ImageUri=image_uri,
        Publish=True,
    )
    # wait for function to finish updating
    waiter = client.get_waiter("function_updated")
    waiter.wait(FunctionName="crowemi-trades-lambda-get-data")
    # update configuration
    response = client.update_function_configuration(
        FunctionName=definition["FunctionName"],
        Environment=definition["Environment"],
    )
    print(response)
except botocore.exceptions.ClientError as e:
    if e.response["Error"]["Code"] == "ResourceNotFoundException":
        response = client.create_function(**definition)
        print(response)
    else:
        raise e
