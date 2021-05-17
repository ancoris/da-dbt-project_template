"""This script will generate compatiable schema.yml files for each table within the datasets of the GCP
project specified. These yml files will be stored in analysis/utilties/schema_files"""

# Python 3.7
# Author: TL (tweaked by JG for windows)
# Execute from project folder. i.e 'python analysis/utilities/magnificent_model_maker.py'
# For windows usage, you will will need a bash utitlity installed, see https://gitforwindows.org/


import json
import os
import time
import shutil
import re
import platform

# OS discovery
system = platform.system().lower()

if system == 'windows':
    import shlex  # JG
    import subprocess  # JG
    from subprocess import Popen, PIPE, STDOUT  # JG


# Controls console output
output = True

startTime = time.time()

# Specify project id
projectId = "project_here"

# list target bq source datasets
targets = ["database_1", "database_2"]

# IF ON WINDOWS specify path to git for windows sh.exe
git_sh_path = 'C:\\Users\\jgreen\\AppData\\Local\\Programs\\Git\\bin\\sh.exe'

# formatting options
indent = "        "
charCount = 45
charCountClean = 60

##


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def rchop(s, suffix):
    if suffix and s.lower().endswith(suffix.lower()):
        return s[:-len(suffix)]
    return s


#
if system == 'darwin':
    placedParentDir, filename = os.path.split(__file__)
    scriptFolder = placedParentDir + "/"

elif system == 'windows':
    # JG
    placedParentDir = os.getcwd()
    filename = __file__
    scriptFolder = placedParentDir + "/"
    scriptFolder = scriptFolder.replace("\\", "/")

else:
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
    #print("Error: {} : {}".format(scriptFolder+"columns", e.strerror))
    print("Nothing to clean up. {} does not exist".format(
        scriptFolder+"columns"))

# Creates directories given the above
if not os.path.exists(os.path.dirname(scriptFolder+shlocation)):
    os.makedirs(os.path.dirname(scriptFolder+shlocation), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+jsonDump)):
    os.makedirs(os.path.dirname(scriptFolder+jsonDump), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+tableDump)):
    os.makedirs(os.path.dirname(scriptFolder+tableDump), exist_ok=True)

if not os.path.exists(os.path.dirname(scriptFolder+columnDump)):
    os.makedirs(os.path.dirname(scriptFolder+columnDump), exist_ok=True)

if output:
    print("Detected {} base operating system".format(system))

for targetSource in targets:

    # MacOS
    if system == 'darwin':
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

    # Windows
    elif system == 'windows':
        #####
        sh_file = "{}{}{}_{}.sh".format(
            scriptFolder, shlocation, tableSh, targetSource)  # JG
        tablelist_file = "{}{}tableList_{}.txt".format(
            scriptFolder, tableDump, targetSource)  # JG
        open(tablelist_file, "w")

        with open(sh_file, "w") as file:
            #file.write("export PATH=$PATH:'C:/Users/jgreen/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin' \n")

            file.write("bq.cmd ls --max_results=10000 {}:{} | awk '{{print $1}}' | tail +3  > {}".format(projectId,
                                                                                                         targetSource, tablelist_file))

        command = '"{}" {}'.format(
            git_sh_path, '--login -i -c "bash \\"' + sh_file + '\\""')

        # command = '"C:\\Users\\jgreen\\AppData\\Local\\Programs\\Git\\bin\\sh.exe" --login -i -c "bash \\"' + sh_file + '\\""'
        command_args = shlex.split(command)
        process = Popen(command_args, stdout=PIPE, stderr=STDOUT, )
        output, err = process.communicate()

        with open("{}{}tableList_{}.txt".format(scriptFolder, tableDump, targetSource), "r") as tables:
            if output:
                print("Extracting tables from dataset: {}".format(targetSource))
            tableList = {}
            tableList[targetSource] = []
            for table in tables:
                tableList[targetSource].append(table.rstrip())

        with open("{}{}{}_{}.sh".format(scriptFolder, shlocation, tablecolSh, targetSource), "w") as f:
            for dataset in tableList.keys():
                for table in tableList[dataset]:
                    if output:
                        print("Extracting columns from: {}.{}".format(
                            targetSource, table))
                    f.write("bq.cmd show --schema {}:{}.{} > {}{}{}.{}.json\n".format(projectId, dataset,
                                                                                      table, scriptFolder, jsonDump, dataset, table))

        sh_file = "{}{}{}_{}.sh".format(
            scriptFolder, shlocation, tablecolSh, targetSource)  # JG

        command = '"{}" {}'.format(
            git_sh_path, '--login -i -c "bash \\"' + sh_file + '\\""')
        #command = '"C:\\Users\\jgreen\\AppData\\Local\\Programs\\Git\\bin\\sh.exe" --login -i -c "bash \\"' + sh_file + '\\""'
        command_args = shlex.split(command)
        process = Popen(command_args, stdout=PIPE, stderr=STDOUT, )
        output, err = process.communicate()
        print(output)
#
#
# from here, operating system does not matter
# Writes output
datasetInfo = {}
counter = set()
for enum, file in enumerate(os.listdir(scriptFolder+jsonDump)):
    datasetName = file.split(".")[0]
    tableName = file.split(".")[1]
    tableNameOrig = tableName

    # make sure the tableName is not plural
    if tableName.lower().endswith("s") and not tableName.lower().endswith("ss"):
        if tableName.lower().endswith("sses"):
            tableName = rchop(tableName, "es")
        else:
            tableName = rchop(tableName, "s")

    if datasetName not in datasetInfo.keys():
        datasetInfo[datasetName] = {"tableCount": 0, "columnCount": 0}
    datasetInfo[datasetName]["tableCount"] += 1

    dsColFolder = datasetName + "/"

    if not os.path.exists(os.path.dirname(scriptFolder+columnDump+dsColFolder)):
        os.makedirs(os.path.dirname(
            scriptFolder+columnDump+dsColFolder), exist_ok=True)

    with open(scriptFolder+jsonDump+"/"+file, "r") as f:
        data = json.load(f)
        with open("{}{}{}{}".format(scriptFolder, columnDump, dsColFolder, "schema.txt"), 'a+') as columnTxt:
            if output:
                print("Generating yml for {}:{}".format(
                    datasetName, tableName))

            if datasetName not in counter:
                columnTxt.write("version: 2\n")
                columnTxt.write("sources:\n")
                columnTxt.write("  - name: {}\n".format(datasetName))
                columnTxt.write("    database: {}\n".format(projectId))
                columnTxt.write("    tables:\n")
            columnTxt.write("      - name: {}\n".format(tableNameOrig))
            columnTxt.write("        columns:\n")
            for dict in data:
                columnTxt.write("          - name: {}\n".format(dict["name"]))
                datasetInfo[datasetName]["columnCount"] += 1
            columnTxt.write("\n")
            counter.add(datasetName)

        # raw #
        if not os.path.exists(os.path.dirname("{}{}{}raw/".format(scriptFolder, columnDump, dsColFolder))):
            os.makedirs(os.path.dirname(
                "{}{}{}raw/".format(scriptFolder, columnDump, dsColFolder)), exist_ok=True)

        with open("{}{}{}raw/{}".format(scriptFolder, columnDump, dsColFolder, tableNameOrig+"_raw"+".sql"), 'w') as columnTxt:
            if output:
                print("Generating select statement for {}:{}".format(
                    datasetName, tableNameOrig))

            columnTxt.write("""{{{{
    config(
        materialized    = 'view',
        schema          = 'raw_{}'
    )
}}}}""".format(datasetName))
            columnTxt.write("\n")
            columnTxt.write("select\n")

            for dict in data:
                # columnTxt.write("{}s.{}{}as {},\n".format(
                #    indent, dict["name"], " "*(charCount - len("s."+dict["name"])), camel_to_snake(dict["name"])))
                columnTxt.write("{}s.{},\n".format(indent, dict["name"]))

            # add raw meta fields
            columnTxt.write("\n")
            columnTxt.write("{}-- meta\n".format(indent))
            columnTxt.write("{}{{{{meta_process_time()}}}}{}as meta_delivery_time,\n".format(
                indent, " "*(charCount - 23)))
            columnTxt.write("{}{{{{meta_process_time()}}}}{}as meta_process_time\n".format(
                indent, " "*(charCount - 23)))

            columnTxt.write(
                "from {{{{ source('{}', '{}') }}}} s\n".format(datasetName, tableNameOrig))

        # clean #
        if not os.path.exists(os.path.dirname("{}{}{}clean/".format(scriptFolder, columnDump, dsColFolder))):
            os.makedirs(os.path.dirname(
                "{}{}{}clean/".format(scriptFolder, columnDump, dsColFolder)), exist_ok=True)

        with open("{}{}{}clean/{}".format(scriptFolder, columnDump, dsColFolder, tableNameOrig+"_clean"+".sql"), 'w') as columnTxt:
            if output:
                print("Generating select statement for {}:{}".format(
                    datasetName, tableNameOrig))

            columnTxt.write("""{{{{
    config(
        materialized    = 'view',
        schema          ='clean_{}'
    )
}}}}\n""".format(datasetName))
            columnTxt.write("\n")
            columnTxt.write("select\n")

            for dict in data:

                # we format timestamps like this:
                #       s.ComplianceStartDate                       as ComplianceStartTime,
                #       cast(s.ComplianceStartDate as date)         as ComplianceStartDate,
                if dict["type"].lower() == "timestamp":
                    colNameTmp = rchop(rchop(dict["name"], "Date"), "Time")
                    ColAliasTime = "{}Time".format(colNameTmp)
                    ColAliasDate = "{}Date".format(colNameTmp)

                    columnTxt.write("\n")

                    # s.ComplianceStartDate                       as ComplianceStartTime,
                    columnTxt.write("{}r.{}{}as {},\n".format(
                        indent, dict["name"], " "*(charCountClean-len("r."+dict["name"])), camel_to_snake(ColAliasTime)))

                    # cast(s.ComplianceStartDate as date)         as ComplianceStartDate,
                    colName = "cast(r.{} as date)".format(dict["name"])
                    columnTxt.write("{}{}{}as {},\n".format(
                        indent, colName, " "*(charCountClean-len(colName)), camel_to_snake(ColAliasDate)))

                    columnTxt.write("\n")
                else:
                    columnTxt.write("{}r.{}{}as {},\n".format(
                        indent, dict["name"], " "*(charCountClean-len("r."+dict["name"])), camel_to_snake(dict["name"])))

            # add clean meta fields
            columnTxt.write("\n")
            columnTxt.write("{}-- meta\n".format(indent))
            columnTxt.write("{}r.meta_delivery_time{}as meta_delivery_time,\n".format(
                indent, " "*(charCountClean - len("r.meta_delivery_time"))))
            columnTxt.write("{}{{{{meta_process_time()}}}}{}as meta_process_time,\n".format(
                indent, " "*(charCountClean - 23)))
            columnTxt.write("{}'{}'{}as meta_source,\n".format(
                indent, datasetName, " "*(charCountClean - len("'"+datasetName+"'"))))
            columnTxt.write("{}1{}as meta_is_valid\n".format(
                indent, " "*(charCountClean - 1)))

            columnTxt.write(
                "from {{{{ ref('{}') }}}} r\n".format(tableNameOrig+"_raw"))
#
#
#
        # dim_events #
        if not os.path.exists(os.path.dirname("{}{}{}dim_events/".format(scriptFolder, columnDump, dsColFolder))):
            os.makedirs(os.path.dirname(
                "{}{}{}dim_events/".format(scriptFolder, columnDump, dsColFolder)), exist_ok=True)

        with open("{}{}{}dim_events/{}".format(scriptFolder, columnDump, dsColFolder, "dim_"+tableName+"_events"+".sql"), 'w') as columnTxt:
            if output:
                print("Generating select statement for {}:{}".format(
                    datasetName, tableName))

            columnTxt.write("""{{
    config(
        materialized    = 'scd2_events',
        schema          = 'pl_reference',
        ignore_deletes  = 'N',
        mode            = 'full',
        check_cols      = [\n""")

            # write out the check cols
            for dict in data:

                if dict["type"].lower() == "timestamp":
                    colNameTmp = rchop(rchop(dict["name"], "Date"), "Time")
                    ColAliasTime = "{}Time".format(colNameTmp)
                    ColAliasDate = "{}Date".format(colNameTmp)

                    columnTxt.write("\n")
                    columnTxt.write("        '{}',\n".format(
                        camel_to_snake(ColAliasTime)))
                    columnTxt.write("        '{}',\n".format(
                        camel_to_snake(ColAliasDate)))
                    columnTxt.write("\n")

                else:
                    columnTxt.write("        '{}',\n".format(
                        camel_to_snake(dict["name"])))
            columnTxt.write("""        ],
        natural_key_col = '{}_natural_key',
        modified_time   = 'meta_process_time',
        created_time    = 'create_date',
        cluster_by      = '{}_natural_key',
        partition_by    = {{'field': 'date(meta_process_time)',
                            'data_type':'date'}}""".format(tableName, tableName))
            columnTxt.write("""
    )
}}\n""")
            columnTxt.write("\n")
            columnTxt.write("select c.{}          as {}_natural_key,  /* CHECK THIS */\n".format(
                camel_to_snake(data[0]["name"]), tableName))

            for dict in data:

                # we format timestamps like this:
                #       s.ComplianceStartDate                       as ComplianceStartTime,
                #       cast(s.ComplianceStartDate as date)         as ComplianceStartDate,
                if dict["type"].lower() == "timestamp":
                    colNameTmp = rchop(rchop(dict["name"], "Date"), "Time")
                    ColAliasTime = "{}Time".format(colNameTmp)
                    ColAliasDate = "{}Date".format(colNameTmp)

                    columnTxt.write("\n")
                    columnTxt.write("{}c.{},\n".format(
                        indent, camel_to_snake(ColAliasTime)))
                    columnTxt.write("{}c.{},\n".format(
                        indent, camel_to_snake(ColAliasDate)))
                    columnTxt.write("\n")
                else:
                    columnTxt.write("{}c.{},\n".format(
                        indent, camel_to_snake(dict["name"])))

            columnTxt.write("{}cast('2000-1-1' as date){}as create_date,\n".format(
                indent, " "*(charCountClean - len("cast('2000-1-1' as date)"))))

            # add clean meta fields
            columnTxt.write("\n")
            columnTxt.write("{}-- meta\n".format(indent))
            columnTxt.write("{}c.meta_source,\n".format(indent))
            columnTxt.write("{}c.meta_delivery_time,\n".format(indent))
            columnTxt.write("{}{{{{meta_process_time()}}}}{}as meta_process_time\n".format(
                indent, " "*(charCountClean - 23)))

            columnTxt.write(
                "from {{{{ ref('{}') }}}} c\n".format(tableNameOrig+"_clean"))
            columnTxt.write(
                "where c.meta_is_valid = 1")

        # dim #
        if not os.path.exists(os.path.dirname("{}{}{}dims/".format(scriptFolder, columnDump, dsColFolder))):
            os.makedirs(os.path.dirname(
                "{}{}{}dims/".format(scriptFolder, columnDump, dsColFolder)), exist_ok=True)

        with open("{}{}{}dims/{}".format(scriptFolder, columnDump, dsColFolder, "dim_"+tableName+".sql"), 'w') as columnTxt:
            if output:
                print("Generating select statement for {}:{}".format(
                    datasetName, tableName))

            columnTxt.write("""{{{{
    config(
        materialized    = 'scd2_history',
        schema          = 'pl_reference',
        natural_key_col = '{}_natural_key',
        cluster_by      = '{}_surrogate_key, {}_natural_key, meta_start_time'
    )
}}}}\n""".format(tableName, tableName, tableName))
            columnTxt.write("select\n")
            columnTxt.write(
                "{}e.{}_surrogate_key,\n".format(indent, tableName))
            columnTxt.write("{}e.{}_natural_key,\n".format(indent, tableName))
            columnTxt.write("\n")
            columnTxt.write("{}-- attributes\n".format(indent))
            columnTxt.write(
                "{}-- CHECK - REMOVE THE NATURAL KEY E.G. E.ID\n".format(indent))
            for dict in data:

                # we format timestamps like this:
                #       s.ComplianceStartDate                       as ComplianceStartTime,
                #       cast(s.ComplianceStartDate as date)         as ComplianceStartDate,
                if dict["type"].lower() == "timestamp":
                    colNameTmp = rchop(rchop(dict["name"], "Date"), "Time")
                    ColAliasTime = "{}Time".format(colNameTmp)
                    ColAliasDate = "{}Date".format(colNameTmp)

                    columnTxt.write("\n")
                    columnTxt.write("{}e.{},\n".format(
                        indent, camel_to_snake(ColAliasTime)))
                    columnTxt.write("{}e.{},\n".format(
                        indent, camel_to_snake(ColAliasDate)))
                    columnTxt.write("\n")
                else:
                    columnTxt.write("{}e.{},\n".format(
                        indent, camel_to_snake(dict["name"])))

            # add clean meta fields
            columnTxt.write("\n")
            columnTxt.write("{}-- meta\n".format(indent))
            columnTxt.write("{}e.meta_process_time,\n".format(indent))
            columnTxt.write("{}e.meta_delivery_time,\n".format(indent))
            columnTxt.write("{}e.meta_scd_action,\n".format(indent))
            columnTxt.write("{}e.meta_start_time,\n".format(indent))
            columnTxt.write(
                "from {{{{ ref('{}') }}}} e\n".format("dim_"+tableName+"_events"))
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
