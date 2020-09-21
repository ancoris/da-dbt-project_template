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


def determine_request_no(datasetId):
    slot_request_query = """select max(request_no) as latest_request_no from `{}.slot_requests` where status = 'Pending cancellation' and is_modified_by_close_slots = 0""".format(
        datasetId)
    request_no = list(client.query(slot_request_query).result())[
        0]['latest_request_no']
    return(int(request_no))


def fetch_status(datasetId, request_no):
    query = """select status from `{}.slot_requests`
            where request_no = {}""".format(datasetId, request_no)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    status = results['status']

    return(status)


def fetch_ids(datasetId, request_no):

    query = """select reservation_id, assignment_id, commitment_id from `{}.slot_requests`
            where request_no = {}""".format(datasetId, request_no)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    assignment_id = results['assignment_id']
    reservation_id = results['reservation_id']
    commitment_id = results['commitment_id']

    return(assignment_id, reservation_id, commitment_id)


def cleanup(assignment_id, reservation_id, commit_id):
    try:
        res_api.delete_assignment(name=assignment_id)
        print("Deleting assignment: {}".format(assignment_id))
    except Exception as e:
        print("Assignment {} does not exist.".format(assignment_id))
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) 
        values('close_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

    try:
        res_api.delete_reservation(name=reservation_id)
        print("Deleting reservation: {}".format(reservation_id))
    except Exception as e:
        print("Reservation {} does not exist.".format(reservation_id))
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('close_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

    try:
        res_api.delete_capacity_commitment(name=commit_id, retry=retry.Retry(
            deadline=90, predicate=Exception, maximum=2))
        print("Deleting commitment : {}".format(commit_id))
    except Exception as e:
        print("Commitment {} does not exist.".format(commit_id))
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('close_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))


def execute(event, context):
    try:
        payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        dataset = payload["protoPayload"]['serviceData']["jobInsertResponse"][
            'resource']['jobConfiguration']['query']['destinationTable']['datasetId']

        request_no = determine_request_no(datasetId=dataset)

        print("Processing slot request number: {}".format(request_no))

        client.query(
            """update `{}.slot_requests` set is_modified_by_close_slots = {} where request_no = {}""".format(dataset, 1, request_no))

        if fetch_status(datasetId=dataset, request_no=request_no) == "Pending cancellation":
            assignment_id, reservation_id, commitment_id = fetch_ids(
                datasetId=dataset, request_no=request_no)

            print("Requested assignment: {}".format(assignment_id))
            print("Requested reservation: {}".format(reservation_id))
            print("Requested commitment: {}".format(commitment_id))

            cleanup(assignment_id, reservation_id, commitment_id)

            # billing status within the environment can take up to 180 seconds to update.
            time.sleep(180)

            client.query("""update `{}.slot_requests`
                    set status = "Cancelled", cancel_time = '{}'
                    where request_no = {}""".format(dataset, datetime.now(timezone.utc), request_no))

            client.query("""update `{}.slot_requests`
                    set duration_mins = TIMESTAMP_DIFF(cancel_time, request_time, minute)
                    where request_no = {}""".format(dataset, request_no))

            print("Successful slot cleanup!")

        else:
            print("Request status is incorrect. Status is currently: {}".format(
                fetch_status(datasetId=dataset)))
            return(None)

    except Exception as e:
        error_client.report_exception()

        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('close_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
