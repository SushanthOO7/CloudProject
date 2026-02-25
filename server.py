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

# For EC2 instances
my_ami_id = "ami-06d2c2b98f0871537"
instance_type = "t3.small"
region = "us-west-2"
max_instances_limit = 15
auth = "web-instance"
security_group = "sg-045fcf946e29b8cb5"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.s3_client = boto3.client("s3")
    app.state.dynamo_client = boto3.client("dynamodb")
    app.state.ec2_client = boto3.client("ec2", region_name = region)
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
        raise HTTPException(500, f"Error : {str(e)}")


@app.post("/create_instance")
async def create_instances():
    instance_ids = []
    ec2_client = app.state.ec2_client

    try:
        for i in range(1, max_instances_limit + 1):
            response = ec2_client.run_instances (
                ImageId = my_ami_id,
                MinCount = 1,
                MaxCount = 1,
                InstanceType = instance_type,
                KeyName = auth,
                SecurityGroupIds = [security_group],
                TagSpecifications = [
                    {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key" : "Name",
                            "Value" : f"app-tier-instance-{i}"
                        },
                        {
                            "Key" : "Role",
                            "Value" : "app-tier"
                        }
                    ]
                }]
            )
            instance_id = response["Instances"][0]["InstanceId"]
            instance_ids.append(instance_id)

        return {
            "status": "Creating !!!",
            "count": len(instance_ids),
            "ids": instance_ids,
            "ami_id": my_ami_id,
        }

    except Exception as e:
        raise HTTPException(500, f"Error : {str(e)}")


@app.get("/instance_status")
async def get_instance_status():
    running = []
    pending = []
    stopped = []

    tag_values = [f"app-tier-instance-{i}" for i in range (1, max_instances_limit + 1)]
    ec2_client = app.state.ec2_client

    try:
        response = ec2_client.describe_instances(
            Filters =
            [
                {
                    "Name": "tag:Name",
                    "Values": tag_values
                }
            ]
        )

        for reservation in response.get("Reservations", []):
            for i in reservation["Instances"]:
                name = next(tag["Value"] for tag in i.get("Tags", []) if tag["Key"] == "Name")

                state = i["State"]["Name"]
                instance_id = i["InstanceId"]

                if state == "running":
                    running.append({"name": name, "id": instance_id})
                elif state == "pending":
                    pending.append({"name": name, "id": instance_id})
                elif state == "stopped":
                    stopped.append({"name": name, "id": instance_id})

        return {
            "running": len(running),
            "pending": len(pending),
            "stopped": len(stopped),
            "running_instances": running,
            "pending_instances": pending,
            "stopped_instances": stopped
        }

    except Exception as e:
        raise HTTPException(500, f"Error : {str(e)}")


@app.delete("/delete_instances")
async def delete_instances():
    ec2_client = app.state.ec2_client
    tag_values = [f"app-tier-instance-{i}" for i in range (1, max_instances_limit + 1)]

    response = ec2_client.describe_instances(
        Filters =
        [
            {
                "Name": "tag:Name",
                "Values": tag_values
            }
        ]
    )

    instance_ids = []
    for reservation in response.get("Reservations", []):
        for i in reservation["Instances"]:
            instance_ids.append(i["InstanceId"])

    if not instance_ids:
        return {"status": "none_found"}

    ec2_client.terminate_instances (
        InstanceIds = instance_ids
    )

    return {
        "status": "Shutting Down",
        "count": len(instance_ids),
        "ids": instance_ids
    }