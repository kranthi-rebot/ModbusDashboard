import json
import time
import logging
import subprocess
import boto3

REGION = "eu-west-1"
CONFIG_FILE = "device.json"

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)
THING_NAME = config.get("pi_id")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobs")

iot = boto3.client("iot-data", region_name=REGION)


def run_command(job_document):
    command = job_document.get("command")
    if not command:
        return False, "No command specified"
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        output = result.stdout + result.stderr
        success = result.returncode == 0
        return success, output
    except Exception as e:
        return False, str(e)


def process_jobs():
    while True:
        try:
            pending = iot.get_pending_job_executions(thingName=THING_NAME)
            jobs = pending.get("queuedJobs", []) + pending.get("inProgressJobs", [])
            for job_desc in jobs:
                job_id = job_desc["jobId"]
                exec_resp = iot.describe_job_execution(
                    jobId=job_id,
                    thingName=THING_NAME,
                    executionNumber=job_desc.get("executionNumber"),
                )
                execution = exec_resp["execution"]
                version = execution["versionNumber"]
                iot.update_job_execution(
                    jobId=job_id,
                    thingName=THING_NAME,
                    executionNumber=execution["executionNumber"],
                    status="IN_PROGRESS",
                    expectedVersion=version,
                )
                job_document = execution.get("jobDocument", {})
                success, output = run_command(job_document)
                status = "SUCCEEDED" if success else "FAILED"
                iot.update_job_execution(
                    jobId=job_id,
                    thingName=THING_NAME,
                    executionNumber=execution["executionNumber"],
                    status=status,
                    statusDetails={"detail": output[:1024]},
                )
                logger.info(f"Job {job_id} completed with status {status}")
        except Exception as e:
            logger.error(f"Error processing jobs: {e}")
        time.sleep(10)


if __name__ == "__main__":
    process_jobs()
