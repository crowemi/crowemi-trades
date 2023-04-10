FROM public.ecr.aws/lambda/python:3.9

RUN yum -y update
RUN yum -y install git

# Copy function code
COPY get_data.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN python -m pip install --upgrade pip
RUN pip install poetry -r requirements.txt

CMD [ "get_data.handler" ]