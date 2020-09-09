# Python 3.7
# https://github.com/dwallace0723/py-dbt-cloud
# https://docs.getdbt.com/dbt-cloud/api

from pydbtcloud import DbtCloud
from google.cloud import error_reporting

# Edit values appropriately
account_id = "account_id"
api_token = "api_token"
job_id = 0000
#


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
        print("Expection triggered. Check stack driver error reporting and cloud_function_error table")
