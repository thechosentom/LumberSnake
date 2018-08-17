import os
import glob
import shutil
import pandas as pd
import time
from datetime import datetime, timedelta
import sys

# IMPORTANT - COMMITS ALL WRITE TO UTF-8 TO AVOID CLASH

reload(sys)
sys.setdefaultencoding('utf-8')

print("Processing ...")

# INITIAL STEP

then = time.time()
size = sum([os.path.getsize(fp) for fp in (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk('.\\Log Dump\\') for f in filenames) if not os.path.islink(fp)])

if not os.path.exists('.\\Log Dump\\'):
    print ("WARNING - the 'Log Dump' folder does not exist. Please read instructions and start again.")
    print ("Exiting...")
    time.sleep(20)
    quit()

# REMOVE OLDER VERSION

if os.path.exists('LumberSnake.csv'):
    os.remove('LumberSnake.csv')

print("Checking Log Dump for files ...")


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

# Start removing lines

if __name__ == '__main__':
    main()

print ("qp-batch processed ...")

# Merge files together

ufiles = glob.glob(r'.\\Log Dump\\*.txt')
get_merged(ufiles).to_csv('LumberSnake.csv')

# Remove old files and make a new Log Dump

shutil.rmtree('.\\Log Dump\\')
os.makedirs('.\\Log Dump\\')

# Record time taken to a text file

now = time.time()
text = "Processed " + str(size) + "bytes in " + str(now - then)[:6] + "scs"
timefile = str(datetime.now().strftime("%Y-%m-%d%H-%M")) + ".txt"

text_file = open(timefile, "w")
text_file.write(text)
text_file.close()
