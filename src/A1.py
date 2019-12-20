import os
import sys

from pyspark.sql.session import SparkSession

from Extractor.TextExtractor import TextExtractor
from Extractor.WarcExtractor import WarcExtractor
from Linking.Linker import Linker
from NLP.SpacyNLP import SpacyNLP
from System import Columns

#java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
#os.environ['JAVA_HOME'] = java8_location

# crashes on cache if this is not enabled (thijmen)
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

ES_HOST = "localhost:9200"
TRIDENT_HOST = "localhost:9090"
WARC_ARCHIVE = "../data/sample.warc.gz"
OUTPUT_FILE = 'results.tsv'

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

def create_spark_app() -> SparkSession:
    return SparkSession \
        .builder \
        .appName("A1") \
        .getOrCreate()

app = create_spark_app()
sc = app.sparkContext

warc_df = WarcExtractor.extract(sc, WARC_ARCHIVE, 'raw.csv')
text_df = TextExtractor.extract(warc_df, 'text.csv').cache()

nlp_df = SpacyNLP.extract(text_df, 'nlp.csv').cache()
link_df = Linker.link(ES_HOST, TRIDENT_HOST, app, nlp_df, 'linked.csv')

df = link_df.toPandas()
with open(OUTPUT_FILE, 'w') as f:
    for _, row in df.iterrows():
        f.write('%s\t%s\t%s\n' % (row[Columns.WARC_ID], row[Columns.NLP_MENTION], row[Columns.FREEBASE_ID]))

os.system('python ../score.py ../data/sample.annotations.tsv %s' % OUTPUT_FILE)
