"""This script will purge datasets from any number of given bigquery projects which contain
any of the specified prefixes"""

# Python 3.7
# Author: TL
# Execute from project folder. i.e 'python analysis/utilities/remove_datasets.py'
# For Windows compatibility try http://www.cygwin.com/

import json
import os
import time
import shutil

debug = True

startTime = time.time()

project_ids = ["cfolab-lush"]
prefixes = ["dbt_tl"]


#
placedParentDir, filename = os.path.split(__file__)
scriptFolder = placedParentDir + "/"

# .sh scripts stored here
datasetSh = "dump/shDatasetDump/"
datasetList = "dump/txtDatasetDump/"
datasetDelete = "dump/deletion/"

if not os.path.exists(os.path.dirname(scriptFolder+datasetSh)):
    os.makedirs(os.path.dirname(scriptFolder+datasetSh), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+datasetList)):
    os.makedirs(os.path.dirname(scriptFolder+datasetList), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+datasetDelete)):
    os.makedirs(os.path.dirname(scriptFolder+datasetDelete), exist_ok=True)

for project in project_ids:
    #####
    with open("{}{}{}.sh".format(scriptFolder, datasetSh, project), "w") as file:
        file.write("bq ls --project_id {} | awk '{{print $1}}' | tail +3  > {}{}{}.txt".format(
            project, scriptFolder, datasetList, project))

    os.system("sh {}{}{}.sh".format(
        scriptFolder, datasetSh, project))

    dataDelete = []
    with open("{}{}{}.txt".format(scriptFolder, datasetList, project), "r") as txt_list:
        for dataset in txt_list:
            if any(prefix in dataset for prefix in prefixes):
                dataDelete.append(dataset.rstrip())
                if debug:
                    print("Deleting: {}:{}".format(project, dataset))

    with open("{}{}{}.sh".format(scriptFolder, datasetDelete, project), "w") as f:
        for dataset in dataDelete:
            f.write("bq rm -r -f -d {}:{}\n".format(project, dataset))

    os.system("sh {}{}{}.sh".format(
        scriptFolder, datasetDelete, project))

try:
    shutil.rmtree(scriptFolder+"dump")
except OSError as e:
    print("Error: {} : {}".format(scriptFolder+"dump", e.strerror))


executionTime = time.time() - startTime
if debug:
    print("-"*25)
    print("Script execution time: {:.2f} seconds".format(executionTime))
