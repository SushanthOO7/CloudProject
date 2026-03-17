from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import PlainTextResponse
from botocore.exceptions import ClientError
from botocore.config import Config
from fastapi.concurrency import run_in_threadpool
from contextlib import asynccontextmanager
import boto3
import logging
import csv
import asyncio

logging.basicConfig(level=logging.INFO)

bucket_name = "1237312494-in-bucket"
dynamo_db_domain_name = "1237312494-dynamoDB"
dataset = "face_images_dataset.csv"

# For EC2 instances
my_ami_id = "ami-0179c5ad94ea17cf7"
instance_type = "t3.small"
region = "us-west-2"
max_instances_limit = 15
auth = "web-instance"
security_group = "sg-045fcf946e29b8cb5"

# For SQS Queue
request_queue = "1237312494-req-queue"
response_queue = "1237312494-resp-queue"

# For response state
response_results: dict = {}
response_events:  dict = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    s3_config = Config(max_pool_connections=50)
    sqs_config = Config(max_pool_connections=50)

    app.state.s3_client = boto3.client("s3", config=s3_config)
    app.state.dynamo_client = boto3.client("dynamodb")
    app.state.ec2_client = boto3.client("ec2", region_name = region)
    sqs = boto3.client("sqs", config=sqs_config)
    app.state.sqs_client= sqs
    app.state.request_queue_url = sqs.get_queue_url(QueueName = request_queue)["QueueUrl"]
    app.state.response_queue_url = sqs.get_queue_url(QueueName = response_queue)["QueueUrl"]

    task = asyncio.create_task(response_consumer(app))
    logging.info("Web Service is up and running !!!")

    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logging.info("Shutting down Web Service !!!")

app = FastAPI (
    lifespan = lifespan
)

async def response_consumer(app: FastAPI) -> None:
    sqs_client = app.state.sqs_client
    response_url = app.state.response_queue_url

    while True:
        try:
            response = await run_in_threadpool(
                sqs_client.receive_message,
                QueueUrl = response_url,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 2,
            )
            for message in response.get("Messages", []):
                body = message["Body"]
                receipt = message["ReceiptHandle"]
                key = body.split(":")[0]

                response_results[key] = body
                await run_in_threadpool(
                    sqs_client.delete_message,
                    QueueUrl = response_url,
                    ReceiptHandle = receipt,
                )
                logging.info(f"[consumer] Deleted response for {key}")

                event = response_events.get(key)
                if event:
                    event.set()

        except asyncio.CancelledError:
            logging.info("[consumer] Shutting down")
            break
        except Exception as e:
            logging.error(f"[consumer] Error: {e}")
            await asyncio.sleep(0.5)

@app.post("/", response_class=PlainTextResponse)
async def image_input_output(inputFile: UploadFile):
    filename = inputFile.filename
    filename_only = filename.rsplit(".", 1)[0]

    if not filename:
        raise HTTPException (
            status_code = 400,
            detail = "Input File is empty"
        )
    if not filename.lower().endswith((".jpg", ".jpeg", ".png")):
        raise HTTPException (
            status_code = 400,
            detail = "Invalid file type"
        )

    file_contents = await inputFile.read()

    event = asyncio.Event()
    response_events[filename_only] = event
    response_results.pop(filename_only, None)

    s3_client = app.state.s3_client
    logging.info("Uploading image to S3 bucket !!!")
    for attempt in range(3):
        try:
            await run_in_threadpool (
                s3_client.put_object,
                Bucket = bucket_name,
                Key = filename,
                Body = file_contents,
            )
            break

        except ClientError as e:
            response_events.pop(filename_only, None)
            logging.error(e)
            raise HTTPException (
                status_code = 500,
                detail = f"Uploading the image to S3 bucket {bucket_name} failed !!!")

        except Exception as e:
            if attempt == 2:
                response_events.pop(filename_only, None)
                raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")
            logging.warning(f"Upload failed, retrying for {attempt + 1} time !")
            await asyncio.sleep(0.5)

    logging.info("Sending message to request queue from web service !!!")
    await send_sqs_message(filename, filename_only)

    # Waiting here for the consumer to deliver result
    try:
        await asyncio.wait_for(event.wait(), timeout = 120)
        result = response_results.pop(filename_only, None)
        if result:
            logging.info(f"Response returned result : {result}")
            return result
        raise HTTPException(
            status_code = 500,
            detail = f"Result missing for {filename}"
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code = 500,
            detail = f"Timeout waiting for {filename}"
        )
    finally:
        response_events.pop(filename_only, None)
        response_results.pop(filename_only, None)

@app.get("/create_table/{table_name}")
async def create_table(table_name):
    dynamo_client = app.state.dynamo_client
    response = dynamo_client.create_table(
    AttributeDefinitions = [
        {
            'AttributeName': 'filename',
            'AttributeType': 'S'
        },
    ],
    TableName = table_name,
    KeySchema = [
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
        response = dynamo_client.get_item (
            TableName = dynamo_db_domain_name,
            Key = {
                "filename": {"S": filename}
            }
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
        response = ec2_client.describe_instances (
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
        return {
            "status" : "none_found"
        }

    ec2_client.terminate_instances (
        InstanceIds = instance_ids
    )

    return {
        "status": "Shutting Down",
        "count": len(instance_ids),
        "ids": instance_ids
    }

async def send_sqs_message(filename, filename_only):
    sqs_client = app.state.sqs_client
    for attempt in range(3):
        try:
            sqs_client.send_message (
                QueueUrl = app.state.request_queue_url,
                MessageBody = filename
            )
            logging.info(f"Message sent successfully : {filename}")
            break

        except ClientError as e:
            response_events.pop(filename_only, None)
            logging.error(e)
            raise HTTPException (
                status_code = 500,
                detail = f"SQS send message operation failed !!! : \n {e}"
            )

        except Exception as e:
            if attempt == 2:
                response_events.pop(filename_only, None)
                raise HTTPException(status_code = 500, detail=f"SQS send failed: {e}")
            logging.warning(f"SQS send message failed, retrying for {attempt + 1} time !")
            await asyncio.sleep(0.5)

@app.delete("/clean_s3")
async def clean_s3_bucket():
    s3_client = app.state.s3_client
    try:
        response = s3_client.list_objects_v2(Bucket = bucket_name)
        objects  = response.get("Contents", [])

        if not objects:
            return {"S3 Bucket ": "already_empty", "Deleted Objects ": 0}

        payload = {
            "Objects": [{"Key": obj["Key"]}
                               for obj in objects]
        }
        s3_client.delete_objects(
            Bucket = bucket_name,
            Delete = payload
        )

        return {"status": "All Objects Cleaned", "deleted": len(objects)}

    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"S3 cleanup failed: {e}")