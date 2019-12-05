import os
import re

from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType

from bs4 import BeautifulSoup, Comment, Doctype

from System import Columns
from Tools.Writer import Writer


class TextExtractor:
    TAGS_TO_REMOVE = [
        'head', 'title',
        'footer', 'header',
        'noscript', 'script',
        'style', 'aside',
        '[document]'
    ]

    @staticmethod
    def __prepare_payload(key, host, payload):
        soup = BeautifulSoup(payload, 'html5lib')

        dir_name = 'beautiful_soup/%s' % key
        os.makedirs(dir_name, exist_ok=True)

        for entry in soup.findAll(text=lambda x: isinstance(x, Comment) or isinstance(x, Doctype)):
            entry.extract()
        for tag in soup(TextExtractor.TAGS_TO_REMOVE):
            tag.extract()  # remove the tags that should be ignored

        lines = list(soup.get_text().split('\n'))
        lines = list(map(lambda x: re.sub(r'[^\x1F-\x7F]+', '', x), lines))
        lines = list(filter(lambda x: x.strip() != "" and "." in x and x.strip().lower() != host.lower(), lines))

        return_t = lines
        return return_t

    @staticmethod
    def extract(warc_df, out_file=""):
        sum_cols = udf(TextExtractor.__prepare_payload, ArrayType(StringType()))
        warc_df = warc_df.withColumn(Columns.WARC_CONTENT, sum_cols(Columns.WARC_ID, Columns.WARC_URL, Columns.WARC_CONTENT))
        warc_df = warc_df.withColumn(Columns.WARC_CONTENT, explode(Columns.WARC_CONTENT))
        if out_file != "":
            Writer.excel_writer(out_file, warc_df)
        return warc_df
