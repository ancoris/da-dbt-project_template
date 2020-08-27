# Python: 3.7

from google.cloud import bigquery
from google.api_core import retry
from google.cloud.bigquery.reservation_v1 import *
from datetime import datetime, timezone
import time
from google.cloud import error_reporting
import json
import base64


admin_project_id = 'psg-magic-dev'
project_id = 'psg-magic-dev'
region = 'US'


res_api = ReservationServiceClient()
parent_arg = "projects/{}/locations/{}".format(admin_project_id,
                                               region)

client = bigquery.Client(project=project_id)

error_client = error_reporting.Client()


def fetch_ids(datasetId):

    query = """select reservation_id, assignment_id, commitment_id from `{}.slot_requests`
            where request_no = (select max(request_no)
            from `{}.slot_requests`)""".format(datasetId, datasetId)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    assignment_id = results['assignment_id']
    reservation_id = results['reservation_id']
    commitment_id = results['commitment_id']

    return(assignment_id, reservation_id, commitment_id)


matfact_check = """select
  user_master_key as user,
  item_id         as item,
  sum_score       as rating"""


def check_matfact_jobs():
    running_scripts = []
    for job in client.list_jobs(max_results=25, state_filter="running", all_users=True):
        if job.job_type == "query":
            # print("{}, {}, {}, {}, {}".format(job.job_id, job.user_email, job.job_type, job.statement_type, job.created))
            if job.statement_type == "SCRIPT" and matfact_check in job.query:
                running_scripts.append((job.job_id, job.created))
        else:
            pass

    if len(running_scripts) >= 1:
        print("Aborting. Running mat fact jobs: {}".format(running_scripts))
        return(True)
    else:
        print("No running mat fact jobs found. Proceeding")
        return(False)


def determine_early_cancellation(datasetId):
    query = """select request_time, max_duration_mins, status from `{}.slot_requests`
            where request_no = (select max(request_no)
            from `{}.slot_requests`)""".format(datasetId, datasetId)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    max_duration_mins = results['max_duration_mins']
    request_time = results['request_time']
    status = results['status']

    time_difference = (
        datetime.now(timezone.utc) - request_time)
    total_seconds = time_difference.total_seconds()
    running_minutes = total_seconds/60

    if status not in ["Cancelled", "Cancelled via polling"]:

        if running_minutes >= max_duration_mins:
            print("Slots have been running for {} minutes longer than allowed. Proceeding".format(
                int(running_minutes-max_duration_mins)))
            return(True)
        else:
            print("Aborting. Slots have been running for {} minutes, {} minutes are allowed before cancelling".format(
                int(running_minutes), max_duration_mins))
            return(False)

    else:
        print("Aborting. Most recent slots have already been cancelled")
        return(False)


def cleanup(assignment_id, reservation_id, commit_id):
    assignments = res_api.list_assignments(parent=reservation_id)
    assignments_list = []
    for assignment in assignments:
        assignments_list.append(assignment.name)

    if assignment_id in assignments_list:
        print("Deleting assignment: {}".format(assignment_id))

        res_api.delete_assignment(name=assignment_id)
    else:
        print("Assignment {} does not exist.".format(assignment_id))
    #
    try:
        res_api.get_reservation(name=reservation_id)
        print("Deleting reservation: {}".format(reservation_id))
        res_api.delete_reservation(name=reservation_id)
    except:
        print("Reservation {} does not exist.".format(reservation_id))
    #
    try:
        res_api.get_capacity_commitment(name=commit_id)
        print("Deleting commitment: {}".format(commit_id))
        res_api.delete_capacity_commitment(name=commit_id,
                                           retry=retry.Retry(deadline=90,
                                                             predicate=Exception,
                                                             maximum=2))
    except:
        print("Slot capacity commitment {} does not exist".format(commit_id))


def execute(event, context):
    try:
        dataset = 'dw_utils'

        if check_matfact_jobs() is False:
            if determine_early_cancellation(datasetId=dataset) is True:
                assignment_id, reservation_id, commitment_id = fetch_ids(
                    datasetId=dataset)

                cleanup(assignment_id, reservation_id, commitment_id)

                time.sleep(210)

                client.query("""update `{}.slot_requests`
                    set status = "Cancelled via polling", cancel_time = '{}'
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, datetime.now(), dataset))

                client.query("""update `{}.slot_requests`
                    set duration_mins = TIMESTAMP_DIFF(cancel_time, request_time, minute)
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, dataset))

            else:
                return(None)

        else:
            return(None)

    except Exception as e:
        error_client.report_exception()

        client.query("insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('close_slots_polling', current_timestamp(), '{}')".format(
            e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")

# I should probably include what slot_request no. it is dealing with via a print.
