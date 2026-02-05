from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import PlainTextResponse
from botocore.exceptions import ClientError
from fastapi.concurrency import run_in_threadpool
from contextlib import asynccontextmanager
import boto3

import logging
import csv

bucket_name = "1237312494-in-bucket"
dynamo_db_domain_name = "1237312494-dynamoDB"
dataset = "face_images_dataset.csv"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.s3_client = boto3.client("s3")
    app.state.dynamo_client = boto3.client("dynamodb")
    logging.info("Application is up and running !!!")
    yield
    logging.info("Shutting down application !!!")

app = FastAPI (
    lifespan = lifespan
)

@app.post("/", response_class=PlainTextResponse)
async def image_input_output(inputFile: UploadFile):
    if not inputFile.filename:
        raise HTTPException (
            status_code = 400,
            detail = "Input File is empty"
        )
    if not inputFile.filename.lower().endswith((".jpg", ".jpeg", ".png")):
        raise HTTPException (
            status_code=400,
            detail="Invalid file type"
        )
    filename_only = inputFile.filename.rsplit(".", 1)[0]
    s3_client = app.state.s3_client
    try:
        await run_in_threadpool(
            s3_client.upload_fileobj,
            inputFile.file,
            bucket_name,
            inputFile.filename
        )
    except ClientError as e:
        logging.error(e)
        raise HTTPException (
            status_code = 500,
            detail = f"Uploading the image to S3 bucket {bucket_name} failed !!!")

    prediction_result = await get_image_name(filename_only)

    return f"{filename_only}:{prediction_result}"

@app.get("/create_table/{table_name}")
async def create_table(table_name):
    dynamo_client = app.state.dynamo_client
    response = dynamo_client.create_table(
    AttributeDefinitions=[
        {
            'AttributeName': 'filename',
            'AttributeType': 'S'
        },
    ],
    TableName= table_name,
    KeySchema=[
        {
            'AttributeName': 'filename',
            'KeyType': 'HASH'
        },
    ],
    BillingMode = 'PAY_PER_REQUEST'
)
    return response

@app.get("/describe_table/{table_name}")
async def describe_table(table_name):
    dynamo_client = app.state.dynamo_client
    response = dynamo_client.describe_table(
        TableName = table_name
)
    return response

@app.get("/populate_table/{table_name}")
async def populate_table(table_name):
    dynamo_client = app.state.dynamo_client
    with open(dataset, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            response = dynamo_client.put_item(
                TableName = table_name,
                Item = {
                    "filename": {"S": row["Image"]},
                    "name": {"S": row["Results"]}
                }
            )
    return response


async def get_image_name(filename):
    dynamo_client = app.state.dynamo_client
    try:
        response = dynamo_client.get_item(
            TableName=dynamo_db_domain_name,
            Key={"filename": {"S": filename}}
        )
        if "Item" not in response:
            raise HTTPException(404, "Image not in dataset")
        return response["Item"]["name"]["S"]
    except Exception as e:
        raise HTTPException(500, str(e))