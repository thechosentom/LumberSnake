# -----------------------------------------------------------------------------
#
# HyperSnake v0.2 - HTTP, QPBatch, and Excp Logs
# Using the template provided as part of the Hyper API to generate a .hyper
# file from the Tableau Server logs.
#
# Only for v2018.2+ of Tableau + Python 3.
# No, I won't support earlier versions. Upgrade already - it's 2019!
#
# -----------------------------------------------------------------------------

import shutil
import os
import glob
import zipfile
import json
import time
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException, print_exception

print("Initialising the HyperSnake...")


# Delete existing Log Dump
def cleanFilepath(directory):
    # clean output file directory
    if os.path.exists(directory):
        print("WARNING - the 'Log Dump' folder exists. Press 'Y' to delete.")
        check = input("Okay to delete? Press 'Y' to confirm | Any other key will exit. >> ")
        if check.lower() == "y":
            shutil.rmtree(directory)
            print("Deleted continuing...")
        else:
            print("Exiting...")
            time.sleep(5)
            quit()

    if os.path.exists('vizql.hyper') or os.path.exists('http.hyper'):
        print("WARNING - an existing output file exists. Press 'Y' to delete.")
        check = input("Okay to delete? Press 'Y' to confirm | Any other key will exit. >> ")
        if check.lower() == "y":
            try:
                os.remove('vizql.hyper')
                os.remove('http.hyper')
                print("Deleted continuing...")
            except Exception as e:
                print(e)
                pass

# EXTRACT LOG FILES FROM SELECTED ZIP
def ExtractLogs(OutputFilepath, ZipLogsFile):
    print('Extracting VizQL Logs From ' + ZipLogsFile + "...")
    zf = zipfile.ZipFile(ZipLogsFile, 'r')

    try:

        for info in zf.infolist():
            if "/nativeapi_vizqlserver" in info.filename:
                print(info.filename)
                if info.filename[-1] == '/':
                    continue
                info.filename = os.path.basename(info.filename)
                zf.extract(info, OutputFilepath+"vizql\\")

            elif "/access" in info.filename:
                print(info.filename)
                if info.filename[-1] == '/':
                    continue
                info.filename = os.path.basename(info.filename)
                zf.extract(info, OutputFilepath+"http\\")

    except Exception as e:
        print (e)
        pass

def VizQLtoHyper(filename, dirpath=''):
    print("Adding " + filename + " to vizql.hyper...")

    path_to_database = Path("vizql.hyper")
    with HyperProcess(telemetry=Telemetry.SEND_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_IF_NOT_EXISTS) as connection:

            path_to_txt = os.path.join(dirpath, filename)

            connection.execute_command(
                command=
                f'''
                CREATE TEMP TABLE raw_log AS (SELECT * from {escape_string_literal(path_to_txt)} (SCHEMA(log_entry json) WITH (FORMAT json)));

                CREATE TABLE IF NOT EXISTS qplog AS (
                SELECT
                   (log_entry->>'ts')::TIMESTAMP AS ts,
                   (log_entry->>'pid') AS pid,
                   (log_entry->>'tid') AS tid,
                   (log_entry->>'req') AS req,
                   (log_entry->>'sev') AS sev,
                   (log_entry->>'sess') AS sess,
                   (log_entry->>'site') AS site,
                   (log_entry->>'user') AS user,
                   (log_entry->'v'->>'elapsed')::DOUBLE PRECISION AS elapsed,
                   (log_entry->'v'->>'elapsed-sum')::DOUBLE PRECISION AS elapsed_sum,
                   (log_entry->'v'->>'job-count')::INT AS job_count,
                   (log_entry->'v'->>'query-errors') AS query_errors,
                   (job_entry->>'elapsed')::DOUBLE PRECISION AS elapsed_jobs,
                   (job_entry->>'fusion-parent') AS fusion_parent,
                   (job_entry->>'owner-component') AS owner_component,
                   (job_entry->>'owner-dashboard') AS owner_dashboard,
                   (job_entry->>'owner-worksheet') AS owner_worksheet,
                   (job_entry->>'query-abstract') AS query_abstract,
                   (job_entry->>'query-id') AS query_id,
                   (queries_entry->>'cache-hit') AS cache_hit,
                   (queries_entry->>'protocol-id') AS protocol_id,
                   (queries_entry->>'query-category') AS query_category,
                   (queries_entry->>'query-compilied') AS query_compiled
                FROM raw_log
                CROSS JOIN json_array_elements(log_entry->'v'->'jobs') as e1(job_entry)
                CROSS JOIN json_array_elements(job_entry->'queries') as e2(queries_entry)
                WHERE log_entry->>'k' = 'qp-batch-summary'
                );

                CREATE TABLE IF NOT EXISTS excplog AS (
                SELECT
                   (log_entry->>'ts')::TIMESTAMP AS ts,
                   (log_entry->>'pid') AS pid,
                   (log_entry->>'tid') AS tid,
                   (log_entry->>'req') AS req,
                   (log_entry->>'sev') AS sev,
                   (log_entry->>'sess') AS sess,
                   (log_entry->>'site') AS site,
                   (log_entry->>'user') AS user,
                   (log_entry->'v'->>'excp-msg') AS excp_msg,
                   (log_entry->'v'->>'excp-type') AS excp_type,
                   (log_entry->'v'->>'msg') AS msg
                FROM raw_log
                WHERE log_entry->>'k' = 'excp'
                )
''')

        print("Job complete." + " "+filename+" has been loaded.")
    #print("The VizQL Hyper Process has been shut down.")

def UnionHTTP(ufiles):
    with open('ApacheSnake.csv', 'wb') as outfile:
        for f in ufiles:
            with open(f, "rb") as infile:
                outfile.write(infile.read())

def HTTPtoHyper():

    print ("Creating http.hyper...")

    path_to_database = Path("http.hyper")
    http_table = TableDefinition(
        name="HTTP",
        columns=[
            TableDefinition.Column("serving host", SqlType.text(), NULLABLE),
            TableDefinition.Column("client host", SqlType.text(), NULLABLE),
            TableDefinition.Column("Authenticated User", SqlType.text(), NULLABLE),
            TableDefinition.Column("Timestamp", SqlType.text(), NULLABLE),
            TableDefinition.Column("Timezone", SqlType.text(), NULLABLE),
            TableDefinition.Column("Port", SqlType.text(), NULLABLE),
            TableDefinition.Column("Request Body", SqlType.text(), NULLABLE),
            TableDefinition.Column("Xforward for", SqlType.text(), NULLABLE),
            TableDefinition.Column("Status Code", SqlType.text(), NULLABLE),
            TableDefinition.Column("Response Size in Bytes", SqlType.text(), NULLABLE),
            TableDefinition.Column("Content Length", SqlType.text(), NULLABLE),
            TableDefinition.Column("Request Time Ms", SqlType.text(), NULLABLE),
            TableDefinition.Column("Request ID", SqlType.text(), NULLABLE)
        ])

    with HyperProcess(telemetry=Telemetry.SEND_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_table(table_definition=http_table)
            path_to_txt = "ApacheSnake.csv"
            count_in_http_table = connection.execute_command(
                command=f"COPY {http_table.name} from {escape_string_literal(path_to_txt)} with "
                f"(format CSV, delimiter ' ', NULL '-', QUOTE '\"', ESCAPE '\\')")
        print("The connection to the Hyper file has been closed.")
    print("The HTTP Hyper Process has been shut down.")

try:
        directory = '.\\Log Dump\\'
        print ("Cleaning current directories and files.")
        cleanFilepath(directory)

        print ("Select your zip logs.")
        Tk().withdraw()
        ZipLogsFile = askopenfilename(initialdir=".\\", title="Select zip logs")
        ExtractLogs(directory, ZipLogsFile)

        #Process VizQL
        try:
            dirpath, _, files = next(os.walk(directory+"vizql\\"))
            for f in files:
                VizQLtoHyper(f, dirpath)

        except Exception as e:
            print (e)
            pass

        #Process HTTP
        try:
            ufiles = glob.glob(r'.\\Log Dump\\http\\*.log')
            UnionHTTP(ufiles)
            HTTPtoHyper()
        except Exception as e:
            print (e)
            pass

        shutil.rmtree(directory)
        os.remove('ApacheSnake.csv')

except HyperException as ex:
        print_exception(ex)
        exit(1)
