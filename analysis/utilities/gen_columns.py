"""This script will generate compatiable schema.yml files for each table within the datasets of the GCP
project specified. These yml files will be stored in analysis/utilties/columns"""

# Python 3.7
# Author: TL
# Execute from project folder. i.e 'python analysis/utilities/gen_columns.py'
# For Windows compatibility try http://www.cygwin.com/

import json
import os
import time
import shutil

# Controls console output
output = True

startTime = time.time()

# Specify project id
projectId = "project_id"


# list target bq source datasets
targets = ["dataset1", "dataset2"]

#
placedParentDir, filename = os.path.split(__file__)
scriptFolder = placedParentDir + "/"


# stores sh scripts for listing tables in a dataset
tableSh = "tables"

# stores sh scripts for generating table schemas in a dataset
tablecolSh = "tableCols"

# .sh scripts stored here
shlocation = "dump/shDump/"

# table lists for each dataset stored here
tableDump = "dump/tableDump/"

# schema .jsons stored here
jsonDump = "dump/jsonDump/"

# schema.yml compatible table columns stored here
columnDump = "schema_files/"

# pre-clean up
try:
    shutil.rmtree(scriptFolder+columnDump)
except OSError as e:
    print("Error: {} : {}".format(scriptFolder+"columns", e.strerror))

# Creates directories given the above
if not os.path.exists(os.path.dirname(scriptFolder+shlocation)):
    os.makedirs(os.path.dirname(scriptFolder+shlocation), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+jsonDump)):
    os.makedirs(os.path.dirname(scriptFolder+jsonDump), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+tableDump)):
    os.makedirs(os.path.dirname(scriptFolder+tableDump), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+columnDump)):
    os.makedirs(os.path.dirname(scriptFolder+columnDump), exist_ok=True)

for targetSource in targets:
    #####
    with open("{}{}{}_{}.sh".format(scriptFolder, shlocation, tableSh, targetSource), "w") as file:
        file.write("bq ls --max_results=10000 {}:{} | awk '{{print $1}}' | tail +3  > {}{}tableList_{}.txt".format(projectId,
                                                                                                                   targetSource, scriptFolder, tableDump, targetSource))

    os.system("sh {}{}{}_{}.sh".format(
        scriptFolder, shlocation, tableSh, targetSource))

    with open("{}{}tableList_{}.txt".format(scriptFolder, tableDump, targetSource), "r") as tables:
        if output:
            print("Extracting tables from dataset: {}".format(targetSource))
        tableList = {}
        tableList[targetSource] = []
        for table in tables:
            tableList[targetSource].append(table.rstrip())

    ######

    with open("{}{}{}_{}.sh".format(scriptFolder, shlocation, tablecolSh, targetSource), "w") as f:
        for dataset in tableList.keys():
            for table in tableList[dataset]:
                if output:
                    print("Extracting columns from: {}.{}".format(
                        targetSource, table))
                f.write("bq show --schema {}:{}.{} > {}{}{}.{}.json\n".format(projectId, dataset,
                                                                              table, scriptFolder, jsonDump, dataset, table))

    os.system("sh {}{}{}_{}.sh".format(scriptFolder,
                                       shlocation, tablecolSh, targetSource))
    if output:
        print("-"*25)
#
#
#
#  Writes output
datasetInfo = {}
counter = set()
for enum, file in enumerate(os.listdir(scriptFolder+jsonDump)):
    if file.split(".")[0] not in datasetInfo.keys():
        datasetInfo[file.split(".")[0]] = {"tableCount": 0, "columnCount": 0}
    datasetInfo[file.split(".")[0]]["tableCount"] += 1

    dsColFolder = file.split(".")[0] + "/"

    if not os.path.exists(os.path.dirname(scriptFolder+columnDump+dsColFolder)):
        os.makedirs(os.path.dirname(
            scriptFolder+columnDump+dsColFolder), exist_ok=True)

    with open(scriptFolder+jsonDump+"/"+file, "r") as f:
        data = json.load(f)
        with open("{}{}{}{}".format(scriptFolder, columnDump, dsColFolder, "schema.yml"), 'a+') as columnTxt:
            if output:
                print("Generating yml for {}:{}".format(
                    file.split(".")[0], file.split(".")[1]))

            if file.split(".")[0] not in counter:
                columnTxt.write("version: 2\n")
                columnTxt.write("sources:\n")
                columnTxt.write("  - name: {}\n".format(file.split(".")[0]))
                columnTxt.write("    tables:\n")
            columnTxt.write("      - name: {}\n".format(file.split(".")[1]))
            columnTxt.write("        columns:\n")
            for dict in data:
                columnTxt.write("          - name: {}\n".format(dict["name"]))
                datasetInfo[file.split(".")[0]]["columnCount"] += 1
            columnTxt.write("\n")
            counter.add(file.split(".")[0])

        with open("{}{}{}{}".format(scriptFolder, columnDump, dsColFolder, file.split(".")[1]+".txt"), 'w') as columnTxt:
            if output:
                print("Generating select statement for {}:{}".format(
                    file.split(".")[0], file.split(".")[1]))

            columnTxt.write("""{{
    config(
        materialized='view',
        schema='raw_',
    )
}}\n""")
            columnTxt.write("\n")
            columnTxt.write("select\n")

            for dict in data:
                columnTxt.write("   {},\n".format(dict["name"]))

            columnTxt.write(
                "from {{ source('{}', '{}') }}\n".format(file.split(".")[0], file.split(".")[1]))

#
#
#
# Exit console output
executionTime = time.time() - startTime
if output:
    print("-"*25)
    print("Script execution time: {:.2f} seconds".format(executionTime))
    print("Extracted from {} bq datasets".format(len(datasetInfo)))
    print("-"*25)
    for dataset in datasetInfo.keys():
        print("Dataset name: {}".format(dataset))
        print("Number of tables: {}".format(
            datasetInfo[dataset]["tableCount"]))
        print("Number of columns: {}".format(
            datasetInfo[dataset]["columnCount"]))
        print("-"*25)


# Clean up
try:
    shutil.rmtree(scriptFolder+"dump")
except OSError as e:
    print("Error: {} : {}".format(scriptFolder+"dump", e.strerror))
