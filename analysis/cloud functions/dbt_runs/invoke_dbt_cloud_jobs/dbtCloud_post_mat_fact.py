# Python 3.7
# https://github.com/dwallace0723/py-dbt-cloud
# https://docs.getdbt.com/dbt-cloud/api

from pydbtcloud import DbtCloud
from google.cloud import error_reporting
from google.cloud import bigquery
from datetime import datetime, timezone

# Update values appropriately
project_id = 'FILL'
account_id = "FILL"
api_token = "FILL"
job_id = 0000
#

client = bigquery.Client(project=project_id)

error_client = error_reporting.Client(project=project_id)


def execute(event, context):
    try:
        dbtcloud = DbtCloud(account_id=account_id, api_token=api_token)

        response = dbtcloud.run_job(job_id=job_id)

        code = response.get("status").get("code")

        if code == 200:
            print("Success")
        elif code == 400:
            print("Bad Request")
        else:
            print("Unauthorized request or not found")

    except Exception as e:
        error_client.report_exception()
        client.query("""insert into `dw_utils.cloud_function_error` (cloud_function, error_time, error) values ('dbtCloud_post_mat_fact', "{}", "{}")""".format(
            datetime.now(timezone.utc), e))
        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
