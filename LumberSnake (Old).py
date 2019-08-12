#########################################################################################################
# LumberSnake BETA
# Date: 01/05/2019
# NOTE: This script should now work for all versions of Tableau Server
# This script extracts VizQL logs from a Tableau Server ziplogs file and processes them for analysis.
# To run the script, you must have installed Python 3.3 or later.
#########################################################################################################

import json
import os
import shutil
import time
import zipfile
from zipfile import ZipFile
from tkinter import Tk
from tkinter.filedialog import askopenfilename

print("Initialising the Snake...")

# THIS STEP LOGS FACTS RE: THE FILE. LOGGING THE LOGS.
then = time.time()

# REMOVE FILES / DIRECTORIES
if os.path.exists('.\\Log Dump\\'):
    print ("WARNING - the 'Log Dump' folder exists. Press 'Y' to delete.")
    check = input("Okay to delete? Press 'Y' to confirm | Any other key will exit. >> ")
    if check.lower() == "y":
        shutil.rmtree('.\\Log Dump\\')
        print ("Deleted continuing...")
    else:
        print ("Exiting...")
        time.sleep(5)
        quit()

#if os.path.exists('endqp.json') or os.path.exists('qpbatch.json') or os.path.exists('excp.json'):
if os.path.exists('qpbatch.json') or os.path.exists('excp.json'):
    print ("WARNING - an existing output file exists. Press 'Y' to delete.")
    check = input("Okay to delete? Press 'Y' to confirm | Any other key will exit. >> ")
    if check.lower() == "y":
        try:
            #os.remove('endqp.json')
            os.remove('qpbatch.json')
            os.remove('excp.json')
            print ("Deleted continuing...")
        except Exception as e:
            print(e)
    else:
        print ("Exiting...")
        time.sleep(5)
        quit()

'''def cleanFilepath(filepath):
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
'''

# EXTRACT VIZQL FILES FROM SELECTED ZIP (TSM Style)
def ExtractVizqlLogs(OutputFilepath, ZipLogsFile):
    print('Extracting VizQL Logs From ' + ZipLogsFile + "...")
    zf = zipfile.ZipFile(ZipLogsFile, 'r')
    # print( zf.namelist())

    try:

        for info in zf.infolist():
            # print info.filename
            if "/nativeapi_vizqlserver" in info.filename:
                print(info.filename)
                if info.filename[-1] == '/':
                    continue
                info.filename = os.path.basename(info.filename)
                zf.extract(info, OutputFilepath)

    except Exception as e:
        print (e)
        pass

# EXTRACT VIZQL FILES FROM SELECTED ZIP (pre-TSM)
def unpack_zip(zipfile='', path_from_local=''):
    filepath = path_from_local + zipfile
    extract_path = OutputFilepath
    parent_archive = ZipFile(filepath)

    for info in parent_archive.infolist():
        if "/vizqlserver_" in info.filename:
            print (info.filename)
            if info.filename[-1] == '/':
                continue
            info.filename = os.path.basename(info.filename)
            parent_archive.extract(info, extract_path)
        if "worker" in info.filename:
            print (info.filename)
            info.filename = os.path.basename(info.filename)
            parent_archive.extract(info, extract_path)

    namelist = parent_archive.namelist()
    parent_archive.close()

    for name in namelist:
        try:
            if name[-4:] == '.zip':
                filepath = './Log Dump/' + name
                sub_archive = ZipFile(filepath)

                for info in sub_archive.infolist():
                    # print info.filename
                    if "/vizqlserver_" in info.filename:
                        print (info.filename)
                        if info.filename[-1] == '/':
                            continue
                        info.filename = os.path.basename(info.filename)
                        path= './Log Dump/'+os.path.basename(filepath.strip('.zip'))+'/'
                        sub_archive.extract(info, path)

                rdir = os.getcwd() + '\Log Dump\\'
                filelist = []
                for tree, fol, fils in os.walk(rdir):
                    filelist.extend([os.path.join(tree, fil) for fil in fils if fil.endswith('.txt')])
                for cnt, fil in enumerate(filelist):
                    os.rename(fil, os.path.join(rdir, str(cnt + 1).zfill(2) + '_' + fil[fil.rfind('\\') + 1:]))

                print ("Successfully extracted " + name + "!")

        except Exception as e:

            print ('failed on', name)
            print (e)
            pass

    return extract_path

# CALL FORTH THE LOG SNAKE
def LumberSnake(filename, dirpath=''):
    filename = os.path.join(dirpath, filename)

    excp = open('excp.json', 'a+')
    qpbatch = open('qpbatch.json', 'a+')
    #endqp = open('endqp.json', 'a+')

    try:
        with open(filename, 'r') as f_read:
            for line in f_read:
                message = json.loads(line)
                if message["k"] == 'excp':
                    #print(message)
                    json.dump(message, excp)
                    #excp.write(json.dumps(message))
                    excp.write('\n')

                elif message["k"] == 'qp-batch-summary':
                    #print(message)
                    json.dump(message, qpbatch)
                    #qpbatch.write(json.dumps(message))
                    qpbatch.write('\n')

                #elif "end-qp" in message["k"]:
                #    #print(message)
                #    json.dump(message, endqp)
                 #   endqp.write('\n')

        print(filename + " processed successfully!")

    except Exception as e:
        print (filename + " failed! Check error below.")
        print (e)
        pass

# MAIN PROGRAM VARIABLES
def main():
    directory = '.\\Log Dump\\'
    dirpath, _, files = next(os.walk(directory))

    for f in files:
        LumberSnake(f, dirpath)

#################################################

# SET THE OUTPUT PATH AND ALLOW A USER TO SELECT INPUT
print("Extracting files from zip ...")
OutputFilepath = ".\\Log Dump\\"

## SHOW A SELECT FILE BOX
Tk().withdraw()
ZipLogsFile = askopenfilename(initialdir=".\\", title="Select zip logs")

## CLEAN FILEPATH AND EXTRACT LOGS FROM ZIP
#cleanFilepath(OutputFilepath)

ExtractVizqlLogs(OutputFilepath, ZipLogsFile)
size = sum([os.path.getsize(fp) for fp in
            (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk(OutputFilepath) for f in filenames)
            if not os.path.islink(fp)]) / 1000000

if size==0:
    path = unpack_zip(ZipLogsFile)

print ("Log files extracted - processing...")

if __name__ == '__main__':
    main()

# LOG TIME TAKEN TO A TEXT FILE
now = time.time()
text = "Processed " + str(size) + "mbs in " + str(now - then)[:6] + "scs"
timefile = "LSLog" + ".txt"
text_file = open(timefile, "a")
text_file.write(text)
text_file.close()

# REMOVE THE LOGDUMP
shutil.rmtree(OutputFilepath)
