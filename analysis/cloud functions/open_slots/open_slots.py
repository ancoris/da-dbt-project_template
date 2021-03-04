# Python: 3.7

from google.cloud import bigquery
from google.cloud.bigquery.reservation_v1 import *
import time
import json
import base64
from google.cloud import error_reporting
from datetime import datetime, timezone


defensive_checks = True

admin_project_id = 'project_id'
project_id = 'project_id'
region = 'US'


client = bigquery.Client(project=project_id)
error_client = error_reporting.Client(project=project_id)

res_api = ReservationServiceClient()
parent_arg = "projects/{}/locations/{}".format(admin_project_id, region)


def determine_request_no(datasetId):
    slot_request_query = """select max(request_no) as latest_request_no from `{}.slot_requests` where status = 'Pending purchase' and is_modified_by_open_slots = 0""".format(
        datasetId)
    request_no = list(client.query(slot_request_query).result())[
        0]['latest_request_no']

    return(int(request_no))


def determine_slot_count(datasetId, request_no):
    slot_count_query = "select slot_count from `{}.slot_requests` where request_no = {}".format(
        datasetId, request_no)

    slot_count = list(client.query(slot_count_query).result())[0]['slot_count']

    return(int(slot_count))


def determine_reservation(datasetId, request_no):
    reservation_name_query = "select reservation_name from `{}.slot_requests` where request_no = {}".format(
        datasetId, request_no)

    reservation_name = list(client.query(reservation_name_query).result())[
        0]['reservation_name']

    return(reservation_name)


def determine_state(reservation_id):
    """checks if the reservation has an assignment with an active state"""
    try:
        assignments = res_api.list_assignments(
            parent=reservation_id)
        if len(list(assignments)) >= 1:

            state = all(str(i.state) == "State.ACTIVE" for i in assignments)

            print("Assignments state: {}. Overall state: {}".format(
                [str(i.state) for i in assignments], state))

            return(state)

        else:
            return(None)

    except Exception as e:
        print("Failed to determine reservation state - See error reporting")
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('open_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
        return(None)


def purchase_commitment(slots, dataset, request_no):
    try:
        commit_config = CapacityCommitment(plan='FLEX', slot_count=slots)
        commit = res_api.create_capacity_commitment(parent=parent_arg,
                                                    capacity_commitment=commit_config)
        print("commit id: {}".format(commit.name))
        client.query(
            """update `{}.slot_requests` set commitment_id = '{}' where request_no = {}""".format(
                dataset, commit.name, request_no))

        return commit.name

    except Exception as e:
        print("Commitment failed - See error reporting")
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('open_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
        client.query(
            """update `{}.slot_requests` set commitment_id = '{}' where request_no = {}""".format(
                dataset, "commitment-failed", request_no))

        return "commitment-failed"


def create_reservation(reservation_name, slots, dataset, request_no):
    try:
        res_config = Reservation(slot_capacity=slots, ignore_idle_slots=False)
        res = res_api.create_reservation(parent=parent_arg,
                                         reservation_id=reservation_name,
                                         reservation=res_config)
        print("reservation id: {}".format(res.name))
        client.query(
            """update `{}.slot_requests` set reservation_id = '{}' where request_no = {}""".format(
                dataset, res.name, request_no))

        return res.name

    except Exception as e:
        print("Reservation failed - See error reporting")
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('open_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
        client.query(
            """update `{}.slot_requests` set reservation_id = '{}' where request_no = {}""".format(
                dataset, "reservation-failed", request_no))

        return "reservation-failed"


def create_assignment(reservation_id, user_project, dataset, request_no):
    try:
        assign_config = Assignment(job_type='QUERY',
                                   assignee='projects/{}'.format(user_project))
        assign = res_api.create_assignment(parent=reservation_id,
                                           assignment=assign_config)
        print("assignment id: {}".format(assign.name))
        client.query(
            """update `{}.slot_requests` set assignment_id = '{}' where request_no = {}""".format(
                dataset, assign.name, request_no))

        return assign.name

    except Exception as e:
        print("Asssignment failed - See error reporting")
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('open_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
        client.query(
            """update `{}.slot_requests` set assignment_id = '{}' where request_no = {}""".format(
                dataset, "assignment-failed", request_no))

        return "assignment-failed"


def execute(event, context):
    try:
        payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        dataset = payload["protoPayload"]['serviceData']["jobInsertResponse"][
            'resource']['jobConfiguration']['query']['destinationTable']['datasetId']

        # Slot count and reservation name are pre-determined in the slot_requests table
        request_no = determine_request_no(datasetId=dataset)
        slotCount = determine_slot_count(
            datasetId=dataset, request_no=request_no)
        reservation_name = determine_reservation(
            datasetId=dataset, request_no=request_no)

        print("Processing slot request number: {}".format(request_no))

        client.query(
            """update `{}.slot_requests` set is_modified_by_open_slots = {} where request_no = {}""".format(
                dataset, 1, request_no))

        # Default values if criteria is not met. Could regex check the reservation name
        if defensive_checks:
            if slotCount % 500 != 0 or slotCount is None:
                slotCount = 500

            if type(reservation_name) is not str or len(reservation_name) < 5 or reservation_name is None:
                reservation_name = "default-{}".format(
                    int(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        #
        commit_id = purchase_commitment(
            slots=slotCount, dataset=dataset, request_no=request_no)

        res_id = create_reservation(
            reservation_name=reservation_name, slots=slotCount, dataset=dataset, request_no=request_no)

        # Assignment will fail if assignment already exists in project
        assign_id = create_assignment(
            reservation_id=res_id, user_project=project_id, dataset=dataset, request_no=request_no)

        # Slots can take a while to attach even after assignment
        time.sleep(180)

        if determine_state(reservation_id=res_id) is True:

            client.query(
                """update `{}.slot_requests` set status = 'Purchased' where request_no = {}""".format(
                    dataset, request_no))

        # if this triggers the assignment may have failed (i.e it already exists)
        else:
            time.sleep(30)
            client.query(
                """update `{}.slot_requests` set status = 'Purchased' where request_no = {}""".format(
                    dataset, request_no))

    except Exception as e:
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values('open_slots', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))

        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
