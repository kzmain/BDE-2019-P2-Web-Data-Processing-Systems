import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark import SparkContext

from Extractor.TextExtractor import TextExtractor
from Extractor.WarcExtractor import WarcExtractor
from Linking.Linker import Linker
from NLP.SpacyNLP import SpacyNLP
from System import Columns

###     =========================================================================================
###     |                                                                                       |
###     |   This is the main program file, the pipeline starts and ends here.                   |
###     |       This file is responsible for the parallelisation offered by Spark (PySpark).    |
###     |       It offers easy configurability by declaring used variables centrally.           |
###     |                                                                                       |
###     =========================================================================================

#java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
#os.environ['JAVA_HOME'] = java8_location

###     =========================
###     |   Declare constants   |
###     =========================

LOCAL = os.getenv("LOCAL", False)

if not LOCAL:
    ES_HOST = "node001:9200"
    TRIDENT_HOST = "node001:9090"
    WARC_ARCHIVE = "hdfs:/user/bbkruit/sample.warc.gz"
    OUTPUT_FILE = "hdfs:/user/wdps1901/results.tsv"
else:
    ES_HOST = "localhost:9200"
    TRIDENT_HOST = "localhost:9090"
    WARC_ARCHIVE = "data/sample.warc.gz"
    OUTPUT_FILE = 'results.tsv'

CALC_SCORE = os.getenv('CALC_SCORE', False)

###     =========================
###     |    CLI Declaration    |
###     =========================


argv_count = len(sys.argv)
if argv_count == 1:
    print("working with default arguments, run with '--help' to view possible arguments...")
elif argv_count == 2 and sys.argv[1] == "--help":
    print("usage: python3 %s WARC_ARCHIVE OUTPUT_FILE ES_HOST" % __file__)
    sys.exit(0)
elif argv_count == 4:
    WARC_ARCHIVE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    ES_HOST = sys.argv[3]

print("arguments: (WARC_ARCHIVE: '%s', OUTPUT_FILE: '%s', ES_HOST: '%s')"%(WARC_ARCHIVE, OUTPUT_FILE, ES_HOST))

print("running spark...")
print("="*40)

###     ==============================
###     |    Spark Initialisation    |
###     ==============================
def create_spark_app() -> SparkSession:
    conf = SparkSession \
        .builder \
        .appName("A1")

    if not LOCAL:
        conf = conf.master("yarn")

    return conf.getOrCreate()

# Initialise spark
app = create_spark_app()
sc = app.sparkContext # type: SparkContext
# sc.setLogLevel("ALL")

# Extract information from WARC file and parse HTML with TextExtractor (returns string of sentences per webpage)
warc_df = WarcExtractor.extract(sc, WARC_ARCHIVE)
text_df = TextExtractor.extract(warc_df).cache()

# Perform NLP Preprocessing on the extracted text, link to found entities in the Knowledge Base
nlp_df = SpacyNLP.extract(text_df).cache()
link_df = Linker.link(ES_HOST, TRIDENT_HOST, nlp_df) #type: DataFrame


tmp_out_file = OUTPUT_FILE+".tmp"
link_df.write.csv(tmp_out_file, mode="overwrite", sep="\t", header=False)

print("concatinating...")
if LOCAL:
    os.system('cat %s/* > %s'%(tmp_out_file, OUTPUT_FILE))

    if CALC_SCORE:
        os.system('python score.py data/sample.annotations.tsv %s' % (OUTPUT_FILE))
else:
    tmp_file = 'tmp.tsv'

    os.system('hadoop fs -getmerge %s/*.csv %s'%(tmp_out_file, tmp_file))
    os.system('hadoop fs -moveFromLocal %s %s'%(tmp_file, OUTPUT_FILE))

print("out_file: %s"%OUTPUT_FILE)
