import gzip
import pandas as pd
from urllib.parse import urlparse

from System import Columns
from Tools.Writer import Writer


class WarcExtractor:
    SAMPLE_ONLY = True

    HEADER_ID = "WARC-TREC-ID"
    HEADER_URI = "WARC-Target-URI"

    columns = [Columns.WARC_ID, Columns.WARC_URL, Columns.WARC_CONTENT]

    @staticmethod
    def __parse_record(raw_record):
        payload_split = raw_record.split('\n\n')
        warc_header = payload_split[0]
        headers = {}
        for line in warc_header.splitlines():
            split = line.split(': ')
            headers[split[0]] = ': '.join(split[1:])

        if WarcExtractor.HEADER_ID in headers and WarcExtractor.HEADER_URI in headers:
            key = headers[WarcExtractor.HEADER_ID]
            uri = headers[WarcExtractor.HEADER_URI]

            payload = '\n\n'.join(payload_split[2:])  # Remove headers
            if len(payload) < 100: return None  # Not a real article
            return key, urlparse(uri).netloc, payload
        else:
            return None

    @staticmethod
    def __get_raw_records(warc_file):
        payload = ''
        with gzip.open(warc_file, "rt", errors="ignore") as stream:
            for line in stream:
                if line.strip() == "WARC/1.0":
                    yield payload
                    payload = ''
                else:
                    payload += line

    @staticmethod
    def __get_all_records(warc_file):
        warc_df = pd.DataFrame(columns=WarcExtractor.columns)
        for record in WarcExtractor.__get_raw_records(warc_file):
            record = WarcExtractor.__parse_record(record)
            if record:
                print("WarcExtractor: ", record[0])
                warc_df.loc[record[0]] = record
        return warc_df

    @staticmethod
    def extract(spark, warc_file, out_file=""):
        warc_df = WarcExtractor.__get_all_records(warc_file)
        if out_file != "":
            Writer.excel_writer(out_file, warc_df)
        warc_df = spark.createDataFrame(warc_df)

        return warc_df



