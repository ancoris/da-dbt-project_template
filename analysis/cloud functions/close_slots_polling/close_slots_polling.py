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

error_client = error_reporting.Client(project=project_id)


def fetch_ids(datasetId, slot_request):

    query = """select reservation_id, assignment_id, commitment_id from `{}.slot_requests`
            where request_no = {}""".format(datasetId, slot_request)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    assignment_id = results['assignment_id']
    reservation_id = results['reservation_id']
    commitment_id = results['commitment_id']

    return(assignment_id, reservation_id, commitment_id)


# Used to check if the mat fact script is running.
# This must be updated if mat fact model dataset or model is altered.
matfact_check = "create or replace model dw_ml_mat_fact.ml_matrix_factorization"


def check_matfact_jobs():
    """checks if there an active mat fact job in the user project"""
    running_scripts = []
    for job in client.list_jobs(max_results=50, state_filter="running", all_users=True):
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


def determine_early_cancellation(datasetId, slot_request):
    query = """select request_time, max_duration_mins, status from `{}.slot_requests`
            where request_no = {}""".format(datasetId, slot_request)

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

    if status not in ["Cancelled", "Cancelled via polling", "Manual cancel"]:

        if running_minutes >= max_duration_mins:
            print("Slots have been running for {} minutes longer than allowed (slot_request_no = {}). Proceeding".format(
                int(running_minutes-max_duration_mins), slot_request))
            return(True)
        else:
            print("Aborting. Slots have been running for {} minutes, {} minutes are allowed before cancelling (slot_request_no = {})".format(
                int(running_minutes), max_duration_mins, slot_request))
            return(False)

    else:
        print("Aborting. Most recent slots have already been cancelled")
        return(False)


def cleanup(assignment_id, reservation_id, commit_id):
    confirmation = []
    if reservation_id is not None:
        try:
            assignments = res_api.list_assignments(parent=reservation_id)
            assignments_list = [assignment.name for assignment in assignments]

            if assignment_id in assignments_list:
                print("Deleting assignment: {}".format(assignment_id))

                res_api.delete_assignment(name=assignment_id)
                confirmation.append(True)

            elif assignment_id is None and len(assignments_list) >= 1:

                print("Assignment {} not in discovered assignments".format(
                    assignment_id))
                for found_assignment_id in assignments_list:
                    print("Deleting assignment: {}".format(found_assignment_id))
                    res_api.delete_assignment(name=found_assignment_id)
                    time.sleep(2)

            else:
                print("Assignment {} does not exist.".format(assignment_id))
        except Exception as e:
            print("Assignment: {} may not exist based off the provided reservation_id")
            error_client.report_exception()
            client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('close_slots_polling', "{}", "{}")""".format(
                datetime.now(timezone.utc), e))
    #
    try:
        res_api.get_reservation(name=reservation_id)
        print("Deleting reservation: {}".format(reservation_id))
        res_api.delete_reservation(name=reservation_id)
        confirmation.append(True)
    except Exception as e:
        print("Reservation {} does not exist.".format(reservation_id))
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('close_slots_polling', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
    #
    try:
        res_api.get_capacity_commitment(name=commit_id)
        print("Deleting commitment: {}".format(commit_id))
        res_api.delete_capacity_commitment(name=commit_id,
                                           retry=retry.Retry(deadline=90,
                                                             predicate=Exception,
                                                             maximum=2))
        confirmation.append(True)
    except Exception as e:
        print("Slot capacity commitment {} does not exist".format(commit_id))
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('close_slots_polling', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

    if any(confirmation):
        return(True)
    else:
        return(False)


def execute(event, context):
    try:
        dataset = 'dw_utils'

        slot_request_query = client.query("""select request_no from `{}.slot_requests`
            where status not in ("Cancelled", "Cancelled via polling", "Manual cancel")""".format(dataset))
        slot_request_query_results = slot_request_query.result()
        slot_request_open_no = [x[0] for x in slot_request_query_results]
        cancelled_list = []

        if check_matfact_jobs() is False:
            for slot_request_no in slot_request_open_no:
                if determine_early_cancellation(datasetId=dataset, slot_request=slot_request_no) is True:
                    assignment_id, reservation_id, commitment_id = fetch_ids(
                        datasetId=dataset, slot_request=slot_request_no)

                    if cleanup(assignment_id, reservation_id, commitment_id) is True:
                        print("Finished processing request_no = {}, sleeping for 180 seconds before continuing".format(
                            slot_request_no))
                        cancelled_list.append(slot_request_no)
                        time.sleep(180)

            for cancelled_request_no in cancelled_list:

                client.query("""update `{}.slot_requests`
                            set status = "Cancelled via polling", cancel_time = '{}'
                            where request_no = {}""".format(dataset, datetime.now(timezone.utc), cancelled_request_no))

                client.query("""update `{}.slot_requests`
                            set duration_mins = TIMESTAMP_DIFF(cancel_time, request_time, minute)
                            where request_no = {}""".format(dataset, cancelled_request_no))

                time.sleep(2)

        else:
            return(None)

    except Exception as e:
        error_client.report_exception()

        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('close_slots_polling', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
