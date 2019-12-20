import sys
import os 

INPUT_FILE = "hdfs:/user/wdps1901/results.tsv"
OUTPUT_FILE = "results.tsv"

argv_count = len(sys.argv)
if argv_count == 1:
    print("working with default arguments, run with '--help' to view possible arguments...")
elif argv_count == 2 and sys.argv[1] == "--help":
    print("usage: python3 %s INPUT_FILE OUTPUT_FILE" % __file__)
    sys.exit(1)
elif argv_count == 3:
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]

os.system('hadoop fs -getmerge %s/*.csv %s'%(INPUT_FILE, OUTPUT_FILE))