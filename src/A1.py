import os

from pyspark.sql.session import SparkSession

from Extractor.TextExtractor import TextExtractor
from Extractor.WarcExtractor import WarcExtractor
from Linking.Linker import Linker
from NLP.SpacyNLP import SpacyNLP

import pandas as pd
from System import Columns

#java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
#os.environ['JAVA_HOME'] = java8_location

ES_HOST = "localhost:9200"
WARC_ARCHIVE = "../data/sample.warc.gz"
OUTPUT_FILE = 'results.tsv'

def create_spark_app():
    return SparkSession \
            .builder \
            .appName("A1") \
            .getOrCreate()

app = create_spark_app()
sc = app.sparkContext

warc_df = WarcExtractor.extract(sc, WARC_ARCHIVE)
text_df = TextExtractor.extract(warc_df)
nlp_df = SpacyNLP.extract(text_df)
link_df = Linker.link(ES_HOST, nlp_df)

df = link_df.toPandas()
with open(OUTPUT_FILE, 'w') as f:
    for _, row in df.iterrows():
        f.write('%s\t%s\t%s\n'%(row[Columns.WARC_ID], row[Columns.NLP_MENTION], row[Columns.FREEBASE_ID]))

os.system('python ../score.py ../data/sample.annotations.tsv %s'%OUTPUT_FILE)

