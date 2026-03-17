import boto3
import time
import logging
from model.face_recognition import face_match

logging.basicConfig(level=logging.INFO)

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
        logging.error(f"Error : {e}")

def get_dynamo_db_items(filename):
    response = dynamo_table.get_item (
        Key = {
            "filename" : filename
        }
    )
    return response

def check_dynamo_db(filename_only):
    try:
        response = dynamo_table.get_item (
            Key = {
                "filename": filename_only
            }
        )
        if "Item" in response:
            logging.info(f"Filename exists for {filename_only}")
            return response["Item"]["name"]
        return None

    except Exception as e:
        logging.warning(f"DynamoDB check failed !!! : \n {e}")
        return None

def insert_to_dynamo_db(filename_only, face_name):
    try:
        dynamo_table.put_item (
            Item = {
                "filename": filename_only,
                "name": face_name
            }
        )
        logging.info(f"Successfully stored in DynamoDB : {filename_only} - {face_name}")

    except Exception as e:
        logging.error(f"Failed to store in DynamoDB : \n {e}")

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
                WaitTimeSeconds = 5,
                VisibilityTimeout = 90
            )

            if "Messages" not in message_response:
                continue

            logging.info("Request queue message received from web service !!!")
            message = message_response["Messages"][0]
            filename = message["Body"]
            filename_only = filename.rsplit(".", 1)[0]

            start = time.time()

            # Checking the Dynamo DB first
            face_name = check_dynamo_db(filename_only)

            if face_name is None:
                # SInce we didnt find the image in dynamo DB, getting it from S3
                logging.info(f"Fetching {filename} image from S3 bucket")
                image = get_s3_object(filename)

                # Getting prediction from the model
                logging.info("Obtaining image prediction from model !!!")
                face_name = get_model_prediction(image)
                logging.info(f"Model Prediction : {face_name}")

                # Storing in DynamoDB for next iterations
                insert_to_dynamo_db(filename_only, face_name)

                elapsed = time.time() - start
                remaining = 4.0 - elapsed
                if remaining > 0:
                    logging.info(f"Waiting for {remaining:.2f}s")
                    time.sleep(remaining)

            else:
                logging.info(f"Using DynamoDB data : {filename_only} - {face_name}")

            logging.info("Sending message to response queue from web service !!!")

            sqs.send_message (
                QueueUrl = response_queue_url,
                MessageBody = f"{filename_only}:{face_name}"
            )
            logging.info(f"Sent to response queue : {filename_only}:{face_name}")

            sqs.delete_message (
                QueueUrl = request_queue_url,
                ReceiptHandle = message["ReceiptHandle"]
            )
            logging.info(f"Deleted {filename} from request queue")

        except Exception as e:
            logging.error(f"Error : {str(e)}")
            time.sleep(2)

if __name__ == "__main__":
    app_service()