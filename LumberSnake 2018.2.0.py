#########################################################################################################
# LumberSnake v2018.2.0
# NOTE: Code only works with logs from Tableau Server 2018.2.0 and up.
# This script extracts vizql logs from a Tableau Server ziplogs file and processes them for analysis.
# To run the script, you must have installed Python 2.7.X or 3.3 and later.
#########################################################################################################

import os
import glob
import shutil
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
import argparse
import zipfile
from tkinter import Tk
from tkinter.filedialog import askopenfilename

# IMPORTANT - COMMITS ALL WRITE TO UTF-8 TO AVOID CLASH
reload(sys)
sys.setdefaultencoding('utf-8')
print("Initialising ...")

# INITIAL STEP
then = time.time()
size = sum([os.path.getsize(fp) for fp in (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk('.\\Log Dump\\') for f in filenames) if not os.path.islink(fp)])/1000000

# REMOVE OLD FILES || CHECK FOR PREV STRUCTURE

if os.path.exists('.\\Log Dump\\'):
    print ("WARNING - the 'Log Dump' folder exists. Press 'Y' to delete.")
    check = raw_input("Okay to delete? Press 'Y' to confirm | Any other key will exit. >> ")
    if check.lower() == "y":
        shutil.rmtree('.\\Log Dump\\')
        print ("Deleted continuing...")
    else:
        print ("Exiting...")
        time.sleep(20)
        quit()

if os.path.exists('LumberSnake.csv'):
    os.remove('LumberSnake.csv')

def cleanFilepath(filepath):
    # clean output file directory
    print("Cleaning all files in filepath " + filepath)
    try:
        if os.path.isdir(filepath):
            shutil.rmtree(filepath)

        if not os.path.isdir(filepath):
            os.mkdir(filepath)

        return True
    except Exception as e:
        print(e)

def ExtractVizqlLogs(OutputFilepath, ZipLogsFile):
    print('Extracting VizQL Logs From ' + ZipLogsFile + "...")
    zf = zipfile.ZipFile(ZipLogsFile, 'r')
    # print( zf.namelist())

    for info in zf.infolist():
        # print info.filename
        if "/nativeapi_vizqlserver" in info.filename:
            print info.filename
            if info.filename[-1] == '/':
                continue
            info.filename = os.path.basename(info.filename)
            zf.extract(info, OutputFilepath)

# MERGE ALL OUTPUT FILES TOGETHER

def get_merged(files, **kwargs):
    dfoutput = pd.read_csv(files[0], **kwargs)
    for f in ufiles[1:]:
        dfoutput = dfoutput.merge(pd.read_csv(f, **kwargs), how='outer')
    return dfoutput

# FIND FILES || KEEP ONLY KCPU & END QUERY || PROCESS FILES INDIVIDUALLY.

def keep_line_from_file(filename, line_to_keep, dirpath=''):
    filename = os.path.join(dirpath, filename)
    temp_path = os.path.join(dirpath, 'temp.txt')

    with open(filename, 'r') as f_read, open(temp_path, 'w') as temp:
        for line in f_read:
            if line.strip().find(line_to_keep) == -1:
                continue
            if line.strip().find('kcpu') == -1:
                continue
            temp.write(line)

    json_path = filename+'.json'
    os.remove(filename)
    os.rename(temp_path, json_path)

    try:

        #INGEST JSON FROM LINES AND FLATTEN STRUCTURE

        df = pd.read_json(json_path, lines=True)
        df["batchid"] = df.index + 1

        df = (pd.DataFrame(df['v'].values.tolist())
                .add_prefix('v.')
                .join(df.drop('v', 1)))

        df = (pd.DataFrame(df['a'].values.tolist())
                .add_prefix('a.')
                .join(df.drop('a', 1)))

        df = (pd.DataFrame(df['a.res'].values.tolist())
                .add_prefix('res.')
                .join(df.drop('a.res', 1)))

        df = (pd.DataFrame(df['res.alloc'].values.tolist())
                .add_prefix('alloc.')
                .join(df.drop('res.alloc', 1)))

        df = (pd.DataFrame(df['res.free'].values.tolist())
                .add_prefix('free.')
                .join(df.drop('res.free', 1)))

        df = (pd.DataFrame(df['res.kcpu'].values.tolist())
                .add_prefix('kcpu.')
                .join(df.drop('res.kcpu', 1)))

        df = (pd.DataFrame(df['res.ucpu'].values.tolist())
                .add_prefix('ucpu.')
                .join(df.drop('res.ucpu', 1)))

        df.to_csv(filename)
        os.remove(json_path)

        print (filename + " file processed")

    except:
        print (filename + " could not be processed!")

# SCRIPT TO REMOVE ANYTHING OTHER THAN END QUERY

def main():
    directory = '.\\Log Dump\\'
    word = 'end-query'

    dirpath, _, files = next(os.walk(directory))

    for f in files:
        keep_line_from_file(f, word, dirpath)

# SET THE OUTPUT PATH AND ALLOW A USER TO SELECT INPUT

print("Extracting files from zip ...")

OutputFilepath = ".\\Log Dump\\"

# SHOW A SELECT FILE BOX

Tk().withdraw()
ZipLogsFile = askopenfilename(initialdir = ".\\",title = "Select zip logs")

print("File Selected...")

# CLEAN FILEPATH AND EXTRACT LOGS FROM ZIP
cleanFilepath(OutputFilepath)

ExtractVizqlLogs(OutputFilepath, ZipLogsFile)

# Start removing lines

if __name__ == '__main__':
    main()

#main()

print ("end-query processed, now unioning...")

# Merge files together

ufiles = glob.glob(r'.\\Log Dump\\*.txt')
get_merged(ufiles).to_csv('LumberSnake.csv')

# Remove processing files, no longer needed

shutil.rmtree(OutputFilepath)

# Record time taken to a text file

now = time.time()
text = "Processed " + str(size) + "mbs in " + str(now - then)[:6] + "scs"
timefile = str(datetime.now().strftime("%Y-%m-%d%H-%M")) + ".txt"

text_file = open(timefile, "w")
text_file.write(text)
text_file.close()