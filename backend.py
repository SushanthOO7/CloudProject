import boto3
import time
import logging
from model.face_recognition import face_match

bucket_name = "1237312494-in-bucket"
request_queue = "1237312494-req-queue"
response_queue = "1237312494-resp-queue"
dynamo_db_table_name = "1237312494-dynamoDB"

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
dynamo_db = boto3.resource("dynamodb")
dynamo_table = dynamo_db.Table(dynamo_db_table_name)

request_queue_url = sqs.get_queue_url(QueueName=request_queue)["QueueUrl"]
response_queue_url = sqs.get_queue_url(QueueName=response_queue)["QueueUrl"]

def get_model_prediction(image):
    try:
        result = face_match(image)
        return result
    except Exception as e:
        print(f"Error : {e}")

def get_dynamo_db_items(filename):
    response = dynamo_table.get_item (
        Key = {
            "filename" : filename
        }
    )
    return response

def get_s3_object(filename):
    value = s3.get_object (
        Bucket = bucket_name,
        Key = filename
    )
    image = value["Body"].read()
    return image

def app_service():
    logging.info("App Service up and running !!!\n")
    while True:
        try:
            message_response = sqs.receive_message (
                QueueUrl = request_queue_url,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 20,
                VisibilityTimeout = 60
            )

            if "Messages" not in message_response:
                continue

            logging.info("Request queue message received from web service !!!")
            message = message_response["Messages"][0]
            filename = message["Body"]
            filename_only = filename.rsplit(".", 1)[0]

            logging.info(f"Fetching {filename} image from S3 bucket")
            image = get_s3_object(filename)

            logging.info("Obtaining image prediction from model !!!")
            face_name = get_model_prediction(image)
            logging.info(f"Model Prediction : {face_name}")

            logging.info("Sending message to response queue from web service !!!")
            sqs.send_message (
                QueueUrl = response_queue_url,
                MessageBody = f"{filename_only}:{face_name}"
            )

            sqs.delete_message (
                QueueUrl = request_queue_url,
                ReceiptHandle = message["ReceiptHandle"]
            )

            logging.info(f"Completed : {filename}")

        except Exception as e:
            logging.error(f"Error : {str(e)}")
            time.sleep(2)

if __name__ == "__main__":
    app_service()