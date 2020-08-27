# Python: 3.7

from google.cloud import bigquery
from google.api_core import retry
from google.cloud.bigquery.reservation_v1 import *
from datetime import datetime, timezone
import time
import json
import base64


admin_project_id = 'psg-magic-dev'
project_id = 'psg-magic-dev'
region = 'US'

res_api = ReservationServiceClient()

client = bigquery.Client(project=project_id)

# API request(s)
matfact_check = """select
  user_master_key as user,
  item_id         as item,
  sum_score       as rating"""


def check_scripts():
    running_scripts = []
    for job in client.list_jobs(max_results=25, state_filter="running", all_users=True):
        if job.job_type == "query":
            # print("{}, {}, {}, {}, {}".format(job.job_id, job.user_email, job.job_type, job.statement_type, job.created))
            if job.statement_type == "SCRIPT" and matfact_check in job.query:
                running_scripts.append((job.job_id, job.created))
        else:
            pass

    if len(running_scripts) >= 1:
        print("Running scripts: {}".format(running_scripts))
        return(True)
    else:
        print("No running scripts found")
        return(False)

# https://googleapis.dev/python/bigquery/latest/search.html?q=queryjob


datasetId = 'dw_utils'


def early_cancellation(datasetId):
    query = """select request_time, max_duration_mins, status from `{}.slot_requests`
            where request_no = (select max(request_no)
            from `{}.slot_requests`)""".format(datasetId, datasetId)

    query_job = client.query(query)
    results = query_job.result()
    results = list(results)[0]

    # max_duration_mins may be typed as a string..
    max_duration_mins = results['max_duration_mins']
    request_time = results['request_time']
    status = results['status']

    time_difference = (
        datetime.now(timezone.utc) - request_time)
    total_seconds = time_difference.total_seconds()
    running_minutes = total_seconds/60
    print(running_minutes)
    print(type(max_duration_mins), type(running_minutes))

    if status == "Cancelled":

        if running_minutes >= max_duration_mins:
            print("Slots have been running for {} minutes longer than allowed".format(
                int(running_minutes-max_duration_mins)))
            return(True)
        else:
            print("Slots have been running for {} minutes, {} minutes are allowed before cancelling".format(
                running_minutes, max_duration_mins))
            return(False)

    else:
        return(False)


assignment_id = "projects/psg-magic-dev/locations/US/reservations/default-20200818093734/assignments/15149652074406329840"
reservation_id = "projects/psg-magic-dev/locations/US/reservations/default-20200818093734"
assignments = res_api.list_assignments(parent=reservation_id)

assignments_list = []
for assignment in assignments:
    assignments_list.append(assignment.name)

if assignment_id in assignments_list:
    print("Hi")
    res_api.delete_assignment(name=assignment_id)
