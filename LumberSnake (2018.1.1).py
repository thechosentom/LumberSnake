#########################################################################################################
# LumberSnake v2018.1.0
# This script extracts vizql logs from a Tableau Server ziplogs file and processes them for analysis.
# NOTE: Code only works with logs from Tableau Server 2018.1.0 and under.
# NOTE: CPU & Memory results are unavailable for this version.
# To run the script, you must have installed Python 2.7.X or 3.3 and later.
#########################################################################################################

import os
import argparse
import glob
import shutil
import zipfile
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from zipfile import ZipFile

# IMPORTANT - COMMITS ALL WRITE TO UTF-8 TO AVOID CLASH

reload(sys)
sys.setdefaultencoding('utf-8')
print("Begin Processing ...")

# INITIAL STEP

then = time.time()
size = sum([os.path.getsize(fp) for fp in (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk('.\\Log Dump\\') for f in filenames) if not os.path.islink(fp)])/1000000


# REMOVE OLDER VERSION

if os.path.exists('LumberSnake.csv'):
    os.remove('LumberSnake.csv')

print("Checking Log Dump for files ...")

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


# UNZIP FILES AUTOMAGICALLY

def unpack_zip(zipfile='', path_from_local=''):
    filepath = path_from_local + zipfile
    #extract_path = './Log Dump/'+os.path.basename(filepath.strip('.zip'))+'/'
    extract_path = './Log Dump/'
    parent_archive = ZipFile(filepath)

    for info in parent_archive.infolist():
        #print info.filename
        if "/vizqlserver_" in info.filename:
            print info.filename
            if info.filename[-1] == '/':
                continue
            info.filename = os.path.basename(info.filename)
            parent_archive.extract(info, extract_path)
        if "worker" in info.filename:
            print info.filename
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
                        print info.filename
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

                #unpack_zip(zipfile=name, path_from_local='./Log Dump/')
                print ("Successfully extracted " + name + "!")
                #os.remove(extract_path+"/"+name)

        except Exception as e:

            print 'failed on', name
            print (e)
            pass





    return extract_path

# Merges all files together without repeating headers.

def get_merged(files, **kwargs):
    dfoutput = pd.read_csv(files[0], **kwargs)
    for f in ufiles[1:]:
        dfoutput = dfoutput.merge(pd.read_csv(f, **kwargs), how='outer')
    return dfoutput

# Find files, keep only qp-batch, process each individually.

def keep_line_from_file(filename, line_to_keep, dirpath=''):
    filename = os.path.join(dirpath, filename)
    temp_path = os.path.join(dirpath, 'temp.txt')

    with open(filename, 'r') as f_read, open(temp_path, 'w') as temp:
        for line in f_read:
            if line.strip().find(line_to_keep) == -1:
                continue
            temp.write(line)

    json_path = filename+'.json'
    os.remove(filename)
    os.rename(temp_path, json_path)

    try:
        df = pd.read_json(json_path, lines=True)
        df = (pd.DataFrame(df['v'].values.tolist())
                .add_prefix('v.')
                .join(df.drop('v', 1)))

        df = (pd.DataFrame(df['v.jobs'].values.tolist())
                .add_prefix('jobs.')
                .join(df.drop('v.jobs', 1)))

        df["batchid"] = df.index + 1

        df = pd.melt(df, id_vars=['user', 'batchid', 'ts', 'tid', 'site', 'sev', 'sess', 'req', 'pid', 'k', 'v.job-count', 'v.elapsed-sum', 'v.elapsed-compute-keys', 'v.elapsed'],
                          var_name="JobNum", value_name="Job")

        df = df[~df['Job'].isnull()]

        df = pd.concat([df.Job.apply(pd.Series), df.drop('Job', axis=1)], axis=1)
        df.to_csv(filename)
        os.remove(json_path)
        print (filename + " file processed")
    except:
        os.remove(json_path)
        print (filename + " has no data!")

# SCRIPT TO REMOVE ANYTHING OTHER THAN QP-BATCH

def main():
    directory = '.\\Log Dump\\'
    word = 'qp-batch-summary'

    dirpath, _, files = next(os.walk(directory))

    for f in files:
        keep_line_from_file(f, word, dirpath)

#################################################

# DEFINE THE OUTPUT
OutputFilepath = ".\\Log Dump\\"

# CLEAN EXISTING FILES
cleanFilepath(OutputFilepath)

# SHOW A SELECT FILE BOX + START EXTRACTION

Tk().withdraw()
zipfilename = askopenfilename(initialdir = ".\\",title = "Select zip logs")

path = unpack_zip(zipfilename)

# START THE VIZQL EXTRACT PROCESS

if __name__ == '__main__':
    main()

print ("qp-batch processed ...")

# MERGE FILES TOGETHER

ufiles = glob.glob(r'.\\Log Dump\\*.txt')
get_merged(ufiles).to_csv('LumberSnake.csv')

# REMOVE OLD FILES

shutil.rmtree(OutputFilepath)

# RECORD THE TIME TAKEN TO PROCESS THE FILES

now = time.time()
text = "Processed " + str(size) + "bytes in " + str(now - then)[:6] + "scs"
timefile = str(datetime.now().strftime("%Y-%m-%d%H-%M")) + ".txt"

text_file = open(timefile, "w")
text_file.write(text)
text_file.close()