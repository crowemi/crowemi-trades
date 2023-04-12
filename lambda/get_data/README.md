# Process Get Data
This repository stores code for creating an AWS Lambda function to retrieve data from Polygon API and store it on S3 for future analysis.

Environment Variables
BUCKET: the bucket where the lambda should drop files.

POLYGON_KEY: the Polygon API key.

AWS_ACCESS_KEY_ID: the AWS access key for accessing AWS resources.

AWS_SECRET_ACCESS_KEY: he AWS secret access key for accessing AWS resources.

ECR_REGISTRY: the ECR registry where the lamba images is stored.

ECR_REPOSITORY: the ECR repository where the lambda image is stored.

IMAGE_TAG: the image tag to deploy.
---

