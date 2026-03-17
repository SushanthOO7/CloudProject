import boto3
import time
import logging

logging.basicConfig(level=logging.INFO)

request_queue = "1237312494-req-queue"
max_instances_limit = 15
shutdown_time = 1

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

    logging.info(f"Queue Load — Pending: {pending}, In Progress: {in_progress}, Total: {total}")
    return total

def autoscale():
    logging.info("Autoscale Service started !!!")
    all_instances_active = False
    count_time = 0
    while True:
        try:
            instances = get_instance_details()
            load = get_queue_details()

            running_ids = [instance_id
                           for instance_id, state in instances.items()
                           if state == "running"]
            stopped_ids = [instance_id
                           for instance_id, state in instances.items()
                           if state == "stopped"]
            pending_ids = [instance_id
                           for instance_id, state in instances.items()
                           if state == "pending"]
            stopping_ids = [instance_id
                            for instance_id, state in instances.items()
                            if state == "stopping"]

            running = len(running_ids)
            pending = len(pending_ids)
            stopped = len(stopped_ids)
            stopping = len(stopping_ids)

            logging.info (
                f"Running: {running}, "
                f"Pending: {pending}, "
                f"Stopped: {stopped}, "
                f"Stopping: {stopping} "
            )

            # Setting the peak no of instances reached
            if running >= max_instances_limit:
                all_instances_active = True

            # If all the instances are running and there is no load then updating to stop all the insatnces as there is no traffic to our application
            if all_instances_active and running == 0 and pending == 0 and stopping == 0 and load == 0:
                logging.info("Updating to set all the instances to stop state")
                all_instances_active = False
                count_time = 0

            # When we have requests to our applications, starting the instances as needed
            if load > 0:
                count_time = 0
                active = running + pending
                if active < max_instances_limit and stopping == 0:
                    needed = max_instances_limit - active
                    start = stopped_ids[:needed]
                    if start:
                        logging.info(f"Starting {len(start)} instances")
                        ec2.start_instances(InstanceIds = start)
                    elif active == 0:
                        logging.warning("Instances might still be in stopping state, there are no stopped instances !!!")

            # When we have updated and want to stop all the instances we stop it in 10 countdowns slowly
            elif all_instances_active and pending == 0 and stopping == 0:
                count_time += 1
                logging.info(f"Shutting down all instances, Countdown : {count_time} of {shutdown_time}")

                if count_time >= shutdown_time:
                    if running:
                        logging.info(f"Stopping {running} instances")
                        ec2.stop_instances(InstanceIds = running_ids)
                    all_instances_active = False
                    count_time = 0
        except Exception as e:
            logging.error(f"Error: {e}")

        time.sleep(2)

if __name__ == "__main__":
    autoscale()