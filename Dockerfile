FROM python:3.8.10-slim
RUN pip install boto3
WORKDIR app 
COPY .aws/ /.aws/
COPY . .
CMD ["python", "main.py"]