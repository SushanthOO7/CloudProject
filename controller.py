import boto3
import time

request_queue = "1237312494-req-queue"

ec2 = boto3.client("ec2")
sqs = boto3.client("sqs")

req_queue_url = sqs.get_queue_url(QueueName=request_queue)["QueueUrl"]

my_ami_id = "ami-06d2c2b98f0871537"
instance_type = "t3.small"

max_instances_limit = 15

def check_running_instances():
    response = ec2.describe_instances (
        Filters = [
            { "Name" : "tag:Name",
              "Values" : ["app-tier-instance-*"]
            },
            { "Name" : "instance-state-name",
              "Values" : ["running"]
            }
        ]
    )
    count = 0
    for r in response["Reservations"]:
        count += len(r["Instances"])
    return count

def get_queue_details():
    # Req queue: pending = visible messages waiting
    req_attributes = sqs.get_queue_attributes (
        QueueUrl = request_queue,
        AttributeNames = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible"
        ]
    )["Attributes"]
    pending = int(req_attributes["ApproximateNumberOfMessages"])

    # In-progress: req NotVisible (being processed) + resp NotVisible (results pending delivery)
    req_in_progress = int(req_attributes["ApproximateNumberOfMessagesNotVisible"])

    # resp_attrs = sqs.get_queue_attributes(
    #     QueueUrl=RESP_QUEUE_URL,
    #     AttributeNames=["ApproximateNumberOfMessagesNotVisible"]
    # )["Attributes"]
    # resp_in_progress = int(resp_attrs["ApproximateNumberOfMessagesNotVisible"])

    in_progress = req_in_progress
    return pending, in_progress

def autoscale():
    print("Autoscale Started !!")
    while True:

        attributes = sqs.get_queue_attributes(
            QueueUrl = req_queue_url,
            AttributeNames = ["ApproximateNumberOfMessages"]
        )

        queue_size = int(attributes["Attributes"]["ApproximateNumberOfMessages"])

        running_instances = check_running_instances()

        if queue_size == 0 and running_instances > 0:
            # Stop all
            print("Stopping all instances")
            # implement stop logic

        elif queue_size > running_instances and running_instances < max_instances_limit:
            print("Scaling up")
            ec2.run_instances(
                ImageId=my_ami_id,
                InstanceType=instance_type,
                MinCount=1,
                MaxCount=1,
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": f"app-tier-instance-{running_instances+1}"}]
                    }
                ]
            )

        time.sleep(5)

if __name__ == "__main__":
    autoscale()