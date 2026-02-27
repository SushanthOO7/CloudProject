import boto3
import time
import logging

logging.basicConfig(level=logging.INFO)

request_queue = "1237312494-req-queue"
max_instances_limit = 15

ec2 = boto3.client("ec2")
sqs = boto3.client("sqs")

request_queue_url = sqs.get_queue_url(QueueName=request_queue)["QueueUrl"]

def get_instance_details():
    response = ec2.describe_instances (
        Filters = [
            {
                "Name" : "tag:Name",
                "Values": ["app-tier-instance-*"]},
            {
                "Name": "instance-state-name",
                "Values": ["running", "stopped", "pending", "stopping"]
            },
        ]
    )
    instances = {}
    for r in response["Reservations"]:
        for i in r["Instances"]:
            instances[i["InstanceId"]] = i["State"]["Name"]
    return instances

def get_queue_details():
    queue_attributes = sqs.get_queue_attributes (
        QueueUrl = request_queue_url,
        AttributeNames = [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]

    pending = int(queue_attributes["ApproximateNumberOfMessages"])
    in_progress = int(queue_attributes["ApproximateNumberOfMessagesNotVisible"])

    total = pending + in_progress

    logging.info(f"Queue Load — Pending: {pending}, In-Flight: {in_progress}, Total: {total}")
    return total

def autoscale():
    logging.info("Autoscale Service started !!!")
    while True:
        try:
            instances = get_instance_details()
            load = get_queue_details()

            running_ids = [instance_id for instance_id, state in instances.items() if state == "running"]
            stopped_ids = [instance_id for instance_id, state in instances.items() if state == "stopped"]
            pending_ids = [instance_id for instance_id, state in instances.items() if state == "pending"]

            running = len(running_ids) + len(pending_ids)

            required = min(load, max_instances_limit)

            logging.info (
                f"Running: {len(running_ids)}, Pending: {len(pending_ids)}, "
                f"Stopped: {len(stopped_ids)}, Needed: {required}"
            )

            if running < required:
                needed = required - running
                start = stopped_ids[:needed]
                if start:
                    logging.info(f"Starting {len(start)} instances : {start}")
                    ec2.start_instances(InstanceIds = start)
                else:
                    logging.warning("There are no stopped instances to start.")

            elif len(running_ids) > required:
                excess = len(running_ids) - required
                stop = running_ids[:excess]
                if stop:
                    logging.info(f"Stopping {len(stop)} instances : {stop}")
                    ec2.stop_instances(InstanceIds = stop)

        except Exception as e:
            logging.error(f"Error : {e}")

        time.sleep(2)

if __name__ == "__main__":
    autoscale()