# Reports on Confluence workflow status including failures

import argparse
import datetime
import json
import logging
import pathlib
import sys
import tempfile

import boto3


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO
)


S3 = boto3.client("s3")
SFN = boto3.client("stepfunctions")
SNS = boto3.client("sns")
TASKS = [
    "init-workflow-subset",
    "init-workflow-global",
    "combine-data-subset",
    "combine-data-global"
]
TOPIC_STRING = "confluence-reports"

def aggregate_run_data(exe_id, name, temporal_range):
    """Aggregate stats and failure data to create and return job stats and failures."""

    module_data = { "name": name, "temporal_range": temporal_range, "modules": [] }
    retrieve_total_time(exe_id, module_data)
    retrieve_task_data(exe_id, module_data)
    failure_data = retrieve_map_data(exe_id, module_data)

    return module_data, failure_data

def retrieve_total_time(exe_id, module_data):
    """Retrieve total execution time."""

    exe_data = SFN.describe_execution(
        executionArn=exe_id
    )
    # Get current time as the workflow is still execution and stopDate is not populated
    module_data["total_time"] = str(datetime.datetime.now(datetime.timezone.utc) - exe_data["startDate"])

def retrieve_task_data(exe_id, module_data):
    """Retrieve execution data for specific state machine tasks."""

    exe_data = SFN.get_execution_history(
        executionArn=exe_id,
        maxResults=1000
    )

    events = exe_data["events"]

    if "nextToken" in exe_data.keys():
        while exe_data.get("nextToken"):
            exe_data = SFN.get_execution_history(
                executionArn=exe_id,
                maxResults=1000,
                nextToken=exe_data.get("nextToken")
            )
            events.extend(exe_data["events"])

    for event in events:
        if event["type"] == "TaskSucceeded" or event["type"] == "TaskFailed":
            task_stats = get_task_stats(event)
            module_data["modules"].append(task_stats)

    return module_data

def get_task_stats(event):
    """Retreive task stats."""

    event_data = {}
    if "taskSucceededEventDetails" in event.keys():
        event_data = json.loads(event["taskSucceededEventDetails"]["output"])
    if "taskFailedEventDetails" in event.keys():
        try:
            if "output" in event["taskFailedEventDetails"]:
                event_data = json.loads(event["taskFailedEventDetails"]["output"])
        except (KeyError, json.JSONDecodeError):
            logging.warning("Failed to parse output from taskFailedEventDetails")

    task_stats = {}
    if event_data:
        job_name = "-".join(event_data["JobName"].split("-")[1:4])
        if job_name in TASKS:
            start_time = datetime.datetime.fromtimestamp(event_data["CreatedAt"] / 1000, tz=datetime.timezone.utc)
            end_time = datetime.datetime.fromtimestamp(event_data["StoppedAt"] / 1000, tz=datetime.timezone.utc)
            job_time = end_time - start_time
            module_name = job_name.replace("-", " ").title()
            failed = 1 if "taskFailedEventDetails" in event.keys() else 0
            task_stats = {
                "Module": module_name,
                "Total Jobs": 1,
                "Succeeded": 1 if "taskSucceededEventDetails" in event.keys() else 0,
                "Failed": failed,
                "Percentage Failed": (failed/1)*100,
                "Execution Time": str(job_time)
            }
    return task_stats

def retrieve_map_data(exe_id, module_data):
    """Retrieve data for map runs and return failures."""

    map_run_arns = SFN.list_map_runs(
        executionArn=exe_id
    )
    map_run_arns = [ map_run['mapRunArn'] for map_run in map_run_arns['mapRuns']]
    logging.info("Total modules executed: %s", len(map_run_arns))

    failure_data = {}
    for map_run_arn in map_run_arns:
        module_name = map_run_arn.split(":")[-2].split("/")[-1].replace("_", " ").title()
        logging.info("Retrieving data for: %s.", module_name)

        map_run_data = SFN.describe_map_run(mapRunArn=map_run_arn)
        map_stats = get_map_stats(map_run_arn, module_name)
        module_data["modules"].append(map_stats)

        if map_stats["Failed"]:
            failure_data[module_name] = get_failures(map_run_arn)

    return failure_data

def get_map_stats(map_run_arn, module_name):
    """Retrieve run stats for map run."""

    map_run_data = SFN.describe_map_run(mapRunArn=map_run_arn)
    total = map_run_data["executionCounts"]["total"]
    failed = map_run_data["executionCounts"]["failed"]
    
    # Use current time if stopDate doesn't exist
    stop_time = map_run_data.get("stopDate", datetime.datetime.now(datetime.timezone.utc))
    
    return {
        "Module": module_name,
        "Total Jobs": total,
        "Succeeded": map_run_data["executionCounts"]["succeeded"],
        "Failed": failed,
        "Percentage Failed": (failed/total)*100,
        "Execution Time": str(stop_time - map_run_data["startDate"])
    }

def get_failures(map_run_arn):
    """Retrieve failed job indexes and identifiers."""

    map_run_list = SFN.list_executions(
        maxResults=1000,
        mapRunArn=map_run_arn
    )

    executions = map_run_list['executions']
    map_run_name = map_run_arn.split("/")[-1].split(":")[0].replace("_", " ").title()
    logging.info("Checking failures for %s.", map_run_name)

    if "nextToken" in map_run_list.keys():
        while map_run_list.get("nextToken"):
            map_run_list = SFN.list_executions(
                maxResults=1000,
                mapRunArn=map_run_arn,
                nextToken=map_run_list.get("nextToken")
            )
            executions.extend(map_run_list['executions'])
            logging.debug("%s results so far: %s", map_run_name, len(executions))
    logging.info("%s Total executions located: %s", map_run_name, len(executions))

    failures = []
    for execution in executions:
        if execution["status"] == "FAILED":
            exe_data = SFN.describe_execution(
                executionArn=execution["executionArn"]
            )
            input_data = json.loads(exe_data["input"])
            failures.append({
                "Index": input_data["context_index"],
                "Value": input_data["context_value"]
            })

    return failures

def write_module_data(module_data, failure_data, data_dir):
    """Write out module data JSON files to data directory."""

    module_file = data_dir.joinpath("module_data_report.json")
    with open(module_file, "w") as jf:
        json.dump(module_data, jf, indent=2)
    logging.info("Wrote module data: %s.", module_file)

    failure_file = data_dir.joinpath("module_data_failures.json")
    with open(failure_file, "w") as jf:
        json.dump(failure_data, jf, indent=2)
    logging.info("Wrote module data: %s.", failure_file)

    return module_file, failure_file

def upload_module_data(module_file, failure_file, bucket, bucket_key, data_dir):
    """Upload reports to JSON S3 bucket."""

    S3.upload_file(
        str(module_file), 
        bucket, 
        f"{bucket_key}/{module_file.name}", 
        ExtraArgs={"ServerSideEncryption": "AES256"}
    )
    logging.info("Uploaded: %s/%s/%s.", bucket, bucket_key, module_file.name)

    S3.upload_file(
        str(failure_file), 
        bucket, 
        f"{bucket_key}/{failure_file.name}", 
        ExtraArgs={"ServerSideEncryption": "AES256"}
    )
    logging.info("Uploaded: %s/%s/%s.", bucket, bucket_key, failure_file.name)

    return f"{bucket}/{bucket_key}/{module_file.name}", f"{bucket}/{bucket_key}/{failure_file.name}" 

def send_module_report(sos_bucket, run_type, bucket_report, bucket_failures, workflow, name):
    """E-mail report module data to SNS topic and include failure if applicable."""

    total_stats = format_output(module_data, workflow)
    logging.debug(total_stats)

    version = name.split("-")[2]
    sos_s3_files = get_sos_s3(sos_bucket, run_type, version)
    logging.debug(sos_s3_files)

    topics = SNS.list_topics()
    topic_arn = ""
    for topic in topics["Topics"]:
        if TOPIC_STRING in topic["TopicArn"]:
            topic_arn = topic["TopicArn"]

    if topic_arn:
        date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%a %b %d %H:%M:%S %Y")
        if failure:
            subject = f"!FAILURE! Confluence workflow report {date_str} UTC"
            message = "CONFLUENCE WORKFLOW FAILURE.\n\n" \
                + f"To locate failures: See AWS Step Function: {workflow} with execution: {name}.\n\n" \
                + "Please visit the following link for documentation on how to troubleshoot: [https://wiki.jpl.nasa.gov/display/PD/Confluence#Confluence-howtohandleerrors].\n\n"
        else:
            subject = f"Confluence workflow report {date_str} UTC"
            message = "CONFLUENCE WORKFLOW SUCCESSFULLY COMPLETED.\n\n"

        message += total_stats \
            + f"{sos_s3_files}\n" \
            + f"Report written to: s3://{bucket_report}.\n" \
            + f"Failures written to: s3://{bucket_failures}.\n"

        response = SNS.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
        logging.info("Email sent to: %s.", topic_arn)
        succeedeed = True

    else:
        logging.error("No SNS Topic was located; an e-mail notification will not be sent.")
        succeedeed = False

    return succeedeed

def format_output(module_data, workflow):
    """Format output as a string representation of a table for e-mailing."""

    total_jobs = 0
    total_succeeded = 0
    total_failed = 0
    for module in module_data["modules"]:
        total_jobs += module["Total Jobs"]
        total_succeeded += module["Succeeded"]
        total_failed += module["Failed"]
    total_failed_percentage = (total_failed / total_jobs) * 100
    total_execution_time = module_data["total_time"]

    totals = f"{workflow} execution: '{module_data["name"]}'\n\n" \
        + f"- Total Jobs: {'{:,}'.format(total_jobs)}\n" \
        + f"- Total Succeeded: {'{:,}'.format(total_succeeded)}\n" \
        + f"- Total Failed: {'{:,}'.format(total_failed)}\n" \
        + f"- Total Failed Percentage: {'{:,.2f}'.format(total_failed_percentage)}\n" \
        + f"- Total Execution Time: {total_execution_time}\n\n"
    return totals

def get_sos_s3(sos_bucket, run_type, version):
    """Return name of granules created in SOS bucket."""

    s3_files = S3.list_objects_v2(
        Bucket=sos_bucket,
        MaxKeys=1000,
        Prefix=f"{run_type}/{version}"
    )

    if "Contents" in s3_files.keys():
        sos_s3_files = [sos_file["Key"].split("/")[-1] for sos_file in s3_files["Contents"]]

        sos_s3_string = f"SOS granules stored in s3://{sos_bucket}/{run_type}/{version}\n\n"
        for sos_s3_file in sos_s3_files:
            sos_s3_string += f"- {sos_s3_file}\n"

    else:
        sos_s3_string = f"No SOS granules stored in S3://{sos_bucket}/{run_type}/{version}.\n"

    return sos_s3_string

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Report on Confluence workflow state")
    arg_parser.add_argument("-e",
                            "--exeid",
                            type=str,
                            help="Step Function state machine execution ID.")
    arg_parser.add_argument("-t",
                            "--temporalrange",
                            type=str,
                            help="Time parameter used to search Hydrocron.")
    arg_parser.add_argument("-s",
                            "--sosbucket",
                            type=str,
                            help="S3 bucket with SOS granules.")
    arg_parser.add_argument("-r",
                            "--runtype",
                            type=str,
                            choices=["unconstrained", "constrained"],
                            help="Run type for Confluence worfklow.")
    arg_parser.add_argument("-b",
                            "--bucket",
                            type=str,
                            help="S3 bucket to upload reports to.")
    arg_parser.add_argument("-k",
                            "--bucketkey",
                            type=str,
                            help="Unique prefix to upload reports to.")
    arg_parser.add_argument("-f",
                            "--fail",
                            action="store_true",
                            help="Indicates workflow state failure.")
    return arg_parser

if __name__ == "__main__":
    start = datetime.datetime.now()

    arg_parser = create_args()
    args = arg_parser.parse_args()

    exe_id = args.exeid
    workflow = exe_id.split(":")[-2]
    name = exe_id.split(":")[-1]
    temporal_range = args.temporalrange
    sos_bucket = args.sosbucket
    run_type = args.runtype
    bucket = args.bucket
    bucket_key = args.bucketkey
    failure = args.fail

    logging.info("Execution ID: %s", exe_id)
    logging.info("Workflow: %s", workflow)
    logging.info("Name: %s", name)
    logging.info("Temporal range: %s", temporal_range)
    logging.info("SOS bucket: %s", sos_bucket)
    logging.info("Run type: %s", run_type)
    logging.info("S3 bucket: %s", bucket)
    logging.info("Bucket key: %s", bucket_key)
    logging.info("Failure state: %s", failure)

    module_data, failure_data = aggregate_run_data(exe_id, name, temporal_range)

    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = pathlib.Path(temp_dir)
        module_file, failure_file = write_module_data(module_data, failure_data, data_dir)
        bucket_report, bucket_failures = upload_module_data(module_file, failure_file, bucket, bucket_key, data_dir)

    succeeded = send_module_report(sos_bucket, run_type, bucket_report, bucket_failures, workflow, name)
    if not succeeded:
        logging.info("Error encountered; exiting now...")
        sys.exit(1)

    end = datetime.datetime.now()
    logging.info("Elapsed time: %s", end - start)