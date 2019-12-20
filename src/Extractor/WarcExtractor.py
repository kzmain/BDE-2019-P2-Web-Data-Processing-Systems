import gzip
import pandas as pd
from urllib.parse import urlparse

from System import Columns
from Tools.Writer import Writer

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.rdd import RDD

import os

###     ==========================================================================
###     |                                                                        |
###     |   This is the WarcExtractor class                                      |
###     |       It is responsible for extracting the HTML from the               |
###     |       available webpages as provided by the WARC file in A1.py.        |
###     |       It uses the Hadoop HDFS system and reads the WARC file as RDD.   |
###     |                                                                        |
###     ==========================================================================

class WarcExtractor:
    SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 0))

    SIZE_THRESHOLD = 100

    FILE_DELIMETER = "WARC/1.0"

    HEADER_ID = "WARC-TREC-ID"
    HEADER_URI = "WARC-Target-URI"

    # Parse each entry (webpage) from the WARC file.
    @staticmethod
    def __parse_record(entry):
        _, raw_record = entry

        payload_split = raw_record.split('\n\n')

        if len(payload_split) >= 3:
            warc_header = payload_split[0]
            headers = {}

            for line in warc_header.splitlines():
                split = line.split(': ')
                headers[split[0]] = ': '.join(split[1:])
            
            if WarcExtractor.HEADER_ID in headers and WarcExtractor.HEADER_URI in headers:
                key = headers[WarcExtractor.HEADER_ID]
                uri = headers[WarcExtractor.HEADER_URI]

                if WarcExtractor.SAMPLE_SIZE > 0 and int(key.split('-')[3]) > WarcExtractor.SAMPLE_SIZE: return None

                payload = '\n\n'.join(payload_split[2:])  # Remove headers

                if len(payload) >= WarcExtractor.SIZE_THRESHOLD: 
                    host = urlparse(uri).netloc
                    # print('WarcExtractor: ', key, host)

                    return key, host, payload
        return None

    # Read the WARC file as an RDD and let Spark handle the distribution among workers
    @staticmethod
    def extract(sc: SparkContext, warc_file, out_file="") -> DataFrame:
        file_reader = sc.newAPIHadoopFile(
            warc_file,
            'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'org.apache.hadoop.io.Text',
            conf={'textinputformat.record.delimiter': WarcExtractor.FILE_DELIMETER}
        ) # type: RDD

        # Filter and extract the valuable information from 
        # the WARC file in parallel and return as a DataFrame
        warc_df = file_reader \
            .map(WarcExtractor.__parse_record) \
            .filter(lambda x: x is not None) \
            .toDF([Columns.WARC_ID, Columns.WARC_URL, Columns.WARC_CONTENT])

        if out_file != "":
            Writer.csv_writer(out_file, warc_df)

        return warc_df



