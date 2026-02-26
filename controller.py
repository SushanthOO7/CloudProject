import boto3
import time
import logging

request_queue = "1237312494-req-queue"

ec2 = boto3.client("ec2")
sqs = boto3.client("sqs")

request_queue_url = sqs.get_queue_url(QueueName=request_queue)["QueueUrl"]

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
    for i in response["Reservations"]:
        count += len(i["Instances"])
    return count

def get_all_instance_ids():
    response = ec2.describe_instances (
        Filters = [
            {
                "Name" : "tag:Name",
                "Values": ["*"]
            }
        ]
    )

    instance_ids = [
        inst["InstanceId"]
        for res in response["Reservations"]
        for inst in res["Instances"]
    ]

    logging.info(f"Instances Id : {instance_ids}")
    return instance_ids

def get_queue_details():
    request = sqs.get_queue_attributes (
        QueueUrl = request_queue_url,
        AttributeNames = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible"
        ]
    )["Attributes"]

    pending_requests = int(request["ApproximateNumberOfMessages"])
    in_progress_requests = int(request["ApproximateNumberOfMessagesNotVisible"])

    total_requests = pending_requests + in_progress_requests
    logging.info(f"Total load : {total_requests}")
    return total_requests


def start_instances(instance_ids, required):
    if required == 0:
        return

    running = check_running_instances()
    needed = required - running

    if needed <= 0:
        logging.info("All the app tier instances are running !!!")
        return

    stopped = get_stopped_instances(instance_ids)
    start = stopped[:needed]

    if start:
        logging.info(f"Starting {len(start)} instances !!!")
        ec2.start_instances(InstanceIds = start)
    else:
        logging.info("Instances not available !!!")

def get_stopped_instances(instance_ids):
    resp = ec2.describe_instances(InstanceIds = instance_ids)
    stopped_instance_ids = [
        i["InstanceId"]
        for res in resp["Reservations"]
        for i in res["Instances"]
        if i["State"]["Name"] == "stopped"
    ]
    return stopped_instance_ids

def stop_instances(instance_ids, required):
    running = check_running_instances()

    if running <= required:
        return

    extra = running - required

    all_resp = ec2.describe_instances (
        Filters = [
            {
                "Name": "tag:Name",
                "Values": [f"app-tier-instance-*"]
            }
        ]
    )

    running_ids = [
        i["InstanceId"]
        for res in all_resp["Reservations"]
        for i in res["Instances"]
        if i["State"]["Name"] == "running"
    ][:extra]

    if running_ids:
        logging.info(f"Stopping instances : {running_ids}")
        ec2.stop_instances(InstanceIds = running_ids)

def autoscale():
    logging.info("Autoscale Service started !!!")
    instance_ids = get_all_instance_ids()

    while True:
        running = check_running_instances()
        workload = get_queue_details()

        target_instances = min(workload, 15)
        target_instances = max(0, target_instances)

        logging.info(f"Running: {running}, Workload: {workload}, Target: {target_instances}")

        if running < target_instances:
            start_instances(instance_ids, target_instances)
        elif running > target_instances:
            stop_instances(instance_ids, target_instances)

        time.sleep(2)

if __name__ == "__main__":
    autoscale()