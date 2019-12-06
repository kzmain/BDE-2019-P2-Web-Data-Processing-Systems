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

TEST_SIGNAL = True

spark = SparkSession \
            .builder \
            .appName("A1") \
            .getOrCreate()

warc_df = WarcExtractor.extract(spark, "../data/sample.warc.gz", 'raw.csv')
text_df = TextExtractor.extract(warc_df, 'extracted.csv')
nlp_df = SpacyNLP.extract(text_df, 'labelled.csv')
link_df = Linker.link("localhost:9200", nlp_df, 'linked.csv')

df = link_df.toPandas()
df.to_csv('out.csv', index=False)

with open('results.tsv', 'w') as f:
    for _, row in df.iterrows():
        f.write('%s\t%s\t%s\n'%(row[Columns.WARC_ID], row[Columns.NLP_MENTION], row[Columns.FREEBASE_ID]))

