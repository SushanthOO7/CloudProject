import boto3
import time
import os
import logging
from botocore.exceptions import ClientError
from model.face_recognition import face_match

bucket_name = "1237312494-in-bucket"
request_queue = "1237312494-req-queue"
response_queue = "1237312494-resp-queue"
dynamo_db_table_name = "1237312494-dynamoDB"

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
dynamo_db = boto3.resource("dynamodb")
dynamo_table = dynamo_db.Table(dynamo_db_table_name)

req_queue_url = sqs.get_queue_url(QueueName=request_queue)["QueueUrl"]
resp_queue_url = sqs.get_queue_url(QueueName=response_queue)["QueueUrl"]

def fake_model_inference(image_bytes):
    return "Paul"

def app_service():
    print("Application up and running !\n")
    # hitCount = 0
    # while True:
    #     resp = sqsInstance.receive_message(
    #         QueueUrl=sqsRequestQueueURL, MaxNumberOfMessages=1, WaitTimeSeconds=5
    #     )
    #     if "Messages" not in resp:
    #         hitCount += 1
    #         if hitCount >= 2:
    #             if EC2_INSTANCE_ID:
    #                 try:
    #                     ec2Instance.stop_instances(InstanceIds=[EC2_INSTANCE_ID])
    #                 except Exception:
    #                     print("Error occured...")
    #             break
    #         continue
    #     hitCount = 0
    #     makePredictions(resp["Messages"][0])
    while True:
        try:
            message_response = sqs.receive_message(
                QueueUrl=req_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=60
            )

            if "Messages" not in message_response:
                continue

            message = message_response["Messages"][0]
            filename = message["Body"]
            receipt_handle = message["ReceiptHandle"]

            logging.info(f"Processing: {filename}")

            # 🔁 1️⃣ Check DynamoDB first (duplicate prevention)
            response = dynamo_table.get_item(Key={"filename": filename})

            if "Item" in response:
                recognition = response["Item"]["recognition"]
                logging.info("Duplicate request. Skipping inference.")

            else:
                # 🔥 2️⃣ Fetch image directly into memory (no disk write)
                s3_object = s3.get_object(Bucket=bucket_name, Key=filename)
                image_bytes = s3_object["Body"].read()

                # 🔥 3️⃣ Run inference
                recognition = fake_model_inference(image_bytes)

                # 🔥 4️⃣ Conditional write (avoids race conditions)
                try:
                    dynamo_table.put_item(
                        Item={
                            "filename": filename,
                            "recognition": recognition
                        },
                        ConditionExpression="attribute_not_exists(filename)"
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        # Another instance inserted it
                        response = dynamo_table.get_item(Key={"filename": filename})
                        recognition = response["Item"]["recognition"]
                    else:
                        raise

            # 🔥 5️⃣ Send response
            sqs.send_message(
                QueueUrl=resp_queue_url,
                MessageBody=f"{filename}:{recognition}"
            )

            # 🔥 6️⃣ Delete request from queue
            sqs.delete_message(
                QueueUrl=req_queue_url,
                ReceiptHandle=receipt_handle
            )

            logging.info(f"Completed: {filename}")

        except Exception as e:
            logging.error(f"Error: {str(e)}")
            time.sleep(2)


if __name__ == "__main__":
    app_service()