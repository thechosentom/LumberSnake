import os
import os.path
import shutil
import zipfile
import time
from tkinter import Tk
from tkinter.filedialog import askopenfilename, askdirectory
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, escape_string_literal
from pathlib import Path


#############################

# UNZIP FILES (VIZQL / HTTP)#

#############################

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

##############################

# Clean any files that clash #

##############################

def cleanFilepath(directory):

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

    if os.path.exists(hyperfile):
        print("WARNING - an existing LumberSnake.hyper file exists.")
        check = input("How to proceed? Press 'Y' to confirm | Press 'A' to append | Any other key will exit. >> ")

        if check.lower() == "y":
            try:
                os.remove(hyperfile)
            except Exception as e:
                print(e)
                pass
            print("Deleted continuing...")

        elif check.lower() == "a":
            print("Will append to file. Warning, data may be duplicated.")
            pass
        else:
            print("Exiting...")
            time.sleep(5)
            quit()


##############################

# Create the Hyper DB        #

##############################

def HyperCreate():
    print(">>> Creating Hyper File <<<")
    path_to_database = Path(hyperfile)
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_IF_NOT_EXISTS) as connection:
            connection.execute_command(
                command=
                f'''
                BEGIN;
                
                create table if not exists http  (
                    serving_host             text,
                    client_host              text,
                    username                 text,
                    ts                       text,
                    timezone                 text,
                    port                     text,
                    request_body             text,
                    xforward_for             text,
                    status_code              text,
                    response_size            text,
                    content_length           text,
                    request_time_ms          text,
                    request_id               text
                );
                
                create table if not exists dump_table (
                    dump text
                );
                
                COMMIT;
                    ''')

##############################

# Convert VizQL files to Hyper
# Dumps files in to Hyper
# Validates JSON lines
# Flattens JSON in Hyper + builds structure
# Iterates across files

##############################

def HyperSnake(vizqlfile):
    path_to_database = Path(hyperfile)

    print(">>> Ingesting " + vizqlfile)

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,database=path_to_database) as connection:

            connection.execute_command(command=
                f'''
                 CREATE TABLE IF NOT EXISTS dump_table AS (
                    SELECT * from {escape_string_literal(vizqlfile)} 
                    (SCHEMA(dump text)));
                
                CREATE TABLE IF NOT EXISTS raw_log AS (
                  SELECT
                    CAST(dump AS json OR NULL) AS log_entry
                      FROM dump_table
                    );
                    
                COMMIT;              
                ''')
            
            print(">>> Logs ingested!")

            print(">>> Cleaning Hyper...")

            connection.execute_command(command=
            f'''
            TRUNCATE TABLE dump_table;
            DROP TABLE dump_table;
            
            DELETE
            FROM raw_log
            WHERE 
            log_entry IS NOT NULL AND
            log_entry->>'k' <> 'qp-batch-summary';  
            
            COMMIT;
            
            ''')

            print(">>> Converting structure now...")

            connection.execute_command(
                command=
                f'''
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
                );      
    
               COMMIT;
               ''')

            print(">>> Dropping the dump table...")

            connection.execute_command(
                command=
                f'''
                   DROP TABLE raw_log;
                
                   COMMIT;
                   ''')

    print(">>> Hyper step complete...")


##############################

# Access to Hyper
# Imports the 'Access' file directly in to Hyper

##############################

def HTTPtoHyper(accessfile):

    print (">>> Importing Access File " + accessfile + " to Hyper...")

    path_to_database = Path(hyperfile)

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:
            connection.execute_command(
                command=f"COPY http from {escape_string_literal(accessfile)} with "
                f"(format CSV, delimiter ' ', NULL '-', QUOTE '\"', ESCAPE '\\')")

    print("HTTP file imported in to Hyper...")

##############################


start = time.time()



if __name__ == '__main__':

    hyperfile = "LumberSnake.hyper"
    directory = '.\\Log Dump\\'

    print("Cleaning current directories and files.")
    cleanFilepath(directory)

    # Select if you want to extract from zip file or point at directory.
    
    print(">>>> DO YOU WANT TO EXTRACT FROM ZIP - OR DIRECTLY FROM A FOLDER? <<<<")
    
    program = input("Select from the following options: \n >> Z for zip file. \n >> F for folder directory. \n >> Any other key to quit. \n >>")

    if program.lower() == "z":
        print("Select your zip logs.")
        Tk().withdraw()
        ZipLogsFile = askopenfilename(initialdir="./", title="Select zip logs")
        ExtractLogs(directory, ZipLogsFile)
        print ("Logs Extracted from Zip!")
    
    elif program.lower() == "f":
        print("Pick your log folder. For example: C:/ProgramData/Tableau/Tableau Server/data/tabsvc/logs/")
        Tk().withdraw()
        if os.path.exists('C:/ProgramData/Tableau/Tableau Server/data/'):
            directory = askdirectory(initialdir="C:/ProgramData/Tableau/Tableau Server/data/", title="Select log folder")
            print (directory + " selected.")
        else:
            directory = askdirectory(initialdir="./", title="Select log folder")
            print(directory + " selected.")
    else:
        print("Exiting...")
        time.sleep(5)
        quit()


    HyperCreate()

    print(">>> Begin Access File Import <<<")

    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in [f for f in filenames if f.startswith("access")]:
            try:
                accessfile = os.path.join(dirpath, filename)
                HTTPtoHyper(accessfile)
            except Exception as e:
                print (e)
                pass

    print(">>> Begin VizQL File Import <<<")

    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in [f for f in filenames if f.startswith("nativeapi_vizql")]:
            try:
                vizqlfile = os.path.join(dirpath, filename)
                HyperSnake(vizqlfile)
            except Exception as e:
                print (e)
                pass

    if program.lower() == "z":
        shutil.rmtree(directory)

    end = time.time()
    print ("Processed in " + str((end - start)/60)[:6] + "mins.")
    time.sleep(5)
