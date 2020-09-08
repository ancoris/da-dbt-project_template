# Python: 3.7

from google.cloud import bigquery
from google.api_core import retry
from google.cloud.bigquery.reservation_v1 import *
from datetime import datetime
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


def fetch_status(datasetId):
    query = """select status from `{}.slot_requests`
            where request_no = (select max(request_no)
            from `{}.slot_requests`)""".format(datasetId, datasetId)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    status = results['status']

    return(status)


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


def cleanup(assignment_id, reservation_id, commit_id):
    res_api.delete_assignment(name=assignment_id)
    res_api.delete_reservation(name=reservation_id)
    res_api.delete_capacity_commitment(name=commit_id,
                                       retry=retry.Retry(deadline=90,
                                                         predicate=Exception,
                                                         maximum=2))


def execute(event, context):
    try:
        payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        dataset = payload["protoPayload"]['serviceData']["jobInsertResponse"][
            'resource']['jobConfiguration']['query']['destinationTable']['datasetId']

        if fetch_status(datasetId=dataset) == "Pending cancellation":
            assignment_id, reservation_id, commitment_id = fetch_ids(
                datasetId=dataset)

            print("Requested assignment: {}".format(assignment_id))
            print("Requested reservation: {}".format(reservation_id))
            print("Requested commitment: {}".format(commitment_id))

            cleanup(assignment_id, reservation_id, commitment_id)

            time.sleep(180)
            if len(list(res_api.list_assignments(
                    parent=reservation_id))) == 0:

                client.query("""update `{}.slot_requests`
                    set status = "Cancelled", cancel_time = '{}'
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, datetime.now(), dataset))

                client.query("""update `{}.slot_requests`
                    set duration_mins = TIMESTAMP_DIFF(cancel_time, request_time, minute)
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, dataset))

                print("Successful slot cleanup!")

            else:
                time.sleep(30)

                client.query("""update `{}.slot_requests`
                    set status = "Cancelled", cancel_time = '{}'
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, datetime.now(), dataset))

                client.query("""update `{}.slot_requests`
                    set duration_mins = TIMESTAMP_DIFF(cancel_time, request_time, minute)
                    where request_no = (select max(request_no)
                    from `{}.slot_requests`)""".format(dataset, dataset))

                print("Successful slot cleanup!")

        else:
            print("Request status is incorrect. Status is currently: {}".format(
                fetch_status(datasetId=dataset)))
            return(None)

    except Exception as e:
        error_client.report_exception()

        client.query("insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('close_slots', current_timestamp(), '{}')".format(
            e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
