import os

from pyspark.sql.session import SparkSession

from Extractor.TextExtractor import TextExtractor
from Extractor.WarcExtractor import WarcExtractor
from Linking.Linker import Linker
from NLP.SpacyNLP import SpacyNLP

#java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
#os.environ['JAVA_HOME'] = java8_location

TEST_SIGNAL = True

spark = SparkSession \
            .builder \
            .appName("A1") \
            .getOrCreate()

warc_df = WarcExtractor.extract(spark, "../data/sample.warc.gz")
text_df = TextExtractor.extract(warc_df)
nlp_df = SpacyNLP.extract(text_df)
link_df = Linker.link("localhost:9200", nlp_df, "out.txt")

link_df.toPandas().to_csv('out.csv', index=False)
# print()

