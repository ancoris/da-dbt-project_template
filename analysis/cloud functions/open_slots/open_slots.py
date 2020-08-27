# Python: 3.7

from google.cloud import bigquery
from google.cloud.bigquery.reservation_v1 import *
import time
import json
import base64
from google.cloud import error_reporting
from datetime import datetime


defensive_checks = True

admin_project_id = 'psg-magic-dev'
project_id = 'psg-magic-dev'
region = 'US'


client = bigquery.Client(project=project_id)
error_client = error_reporting.Client()

res_api = ReservationServiceClient()
parent_arg = "projects/{}/locations/{}".format(admin_project_id, region)


def determine_slot_count(datasetId):
    slot_count_query = "select slot_count from `{}.slot_requests` where request_no = (select max(request_no) from `{}.slot_requests`)".format(
        datasetId, datasetId)

    slot_count = list(client.query(slot_count_query).result())[0]['slot_count']
    return(int(slot_count))


def determine_reservation(datasetId):
    reservation_name_query = "select reservation_name from `{}.slot_requests` where request_no = (select max(request_no) from `{}.slot_requests`)".format(
        datasetId, datasetId)

    reservation_name = list(client.query(reservation_name_query).result())[
        0]['reservation_name']

    return(reservation_name)


def purchase_commitment(slots):
    commit_config = CapacityCommitment(plan='FLEX', slot_count=slots)
    commit = res_api.create_capacity_commitment(parent=parent_arg,
                                                capacity_commitment=commit_config)
    print("commit id: {}".format(commit.name))
    return commit.name


def determine_state(reservation_id):
    """
    States:
    Active = 'State.ACTIVE'
    """

    assignments = res_api.list_assignments(
        parent=reservation_id)
    if len(list(assignments)) >= 1:

        state = all(str(i.state) == "State.ACTIVE" for i in assignments)

        print("Assignments state: {}. Overall state: {}".format(
            [str(i.state) for i in assignments], state))

        return(state)

    else:
        return(None)


def create_reservation(reservation_name, slots):
    res_config = Reservation(slot_capacity=slots, ignore_idle_slots=False)
    res = res_api.create_reservation(parent=parent_arg,
                                     reservation_id=reservation_name,
                                     reservation=res_config)
    print("reservation id: {}".format(res.name))
    return res.name


def create_assignment(reservation_id, user_project):
    assign_config = Assignment(job_type='QUERY',
                               assignee='projects/{}'.format(user_project))
    assign = res_api.create_assignment(parent=reservation_id,
                                       assignment=assign_config)
    print("assignment id: {}".format(assign.name))
    return assign.name


def execute(event, context):
    try:
        payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        dataset = payload["protoPayload"]['serviceData']["jobInsertResponse"][
            'resource']['jobConfiguration']['query']['destinationTable']['datasetId']

        # Slot count and reservation name are pre-determined in the slot_requests app
        slotCount = determine_slot_count(datasetId=dataset)
        reservation_name = determine_reservation(datasetId=dataset)

        # Default values if criteria is not met. Could regex check the reservation name
        if defensive_checks:
            if slotCount % 500 != 0 or slotCount is None:
                slotCount = 500

            if type(reservation_name) is not str or len(reservation_name) < 5 or reservation_name is None:
                reservation_name = "default-{}".format(
                    int(datetime.now().strftime("%Y%m%d%H%M%S")))

        commit_id = purchase_commitment(slotCount)
        res_id = create_reservation(reservation_name, slotCount)
        assign_id = create_assignment(res_id, project_id)

        # Slots can take a while to attach even after assignment
        time.sleep(180)

        if determine_state(reservation_id=res_id) is True:

            query = "update `{}.slot_requests` set reservation_id = '{}', assignment_id = '{}', commitment_id = '{}', status = 'Purchased' where request_no = (select max(request_no) from `{}.slot_requests`)".format(
                dataset, res_id, assign_id, commit_id, dataset)

            client.query(query)

        else:
            time.sleep(60)
            query = "update `{}.slot_requests` set reservation_id = '{}', assignment_id = '{}', commitment_id = '{}', status = 'Purchased' where request_no = (select max(request_no) from `{}.slot_requests`)".format(
                dataset, res_id, assign_id, commit_id, dataset)

            client.query(query)

    except Exception as e:
        error_client.report_exception()
        client.query("insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('open_slots', current_timestamp(), '{}')".format(
            e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")

        # We may want to add in the request_no which failed. This would involve restructuring as the payload is currently handled within the try and could also
        # be a point of failure.
