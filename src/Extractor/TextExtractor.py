import os
import re

from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType

from bs4 import BeautifulSoup, Comment, Doctype

from System import Columns
from Tools.Writer import Writer

import spacy

###     ======================================================================
###     |                                                                    |
###     |   This is the TextExtractor class                                  |
###     |       It is responsible for parsing the HTML and extracting        |
###     |       the interesting information, providing it as a string of     |
###     |       sentences per crawled webpage as provided by the WARC file   |
###     |                                                                    |
###     ======================================================================

class TextExtractor:
    # Use the SMALL (web_sm instead of web_lg) as the accuracy improvement 
    # is not significant but the execution time impact is considerable.
    nlp = spacy.load("en_core_web_sm")

    # These tags are considered to not contain any valuable information
    # for the NLP Preprocessing step and are thus removed.
    TAGS_TO_REMOVE = [
        'head', 'title',
        'footer', 'header',
        'noscript', 'script',
        'style', 'aside',
        '[document]'
    ]

    SETENCE_SIGNS = [
        '.', '?', '!'
    ]

    # Prepare payload by stripping crawled webpages:
        # strip comments and doctype tags,
        # strip undesired tags (TAGS_TO_REMOVE),
        # strip lines that do no pass regex,
        # strip empty words (no chars or space chars),
        # strip sentence signs (SENTENCE_SIGNS),
        # strip any mentions of host
    @staticmethod
    def __prepare_payload(key, host, payload):
        soup = BeautifulSoup(payload, 'html5lib')

        for entry in soup.findAll(text=lambda x: isinstance(x, Comment) or isinstance(x, Doctype)):
            entry.extract()
        for tag in soup(TextExtractor.TAGS_TO_REMOVE):
            tag.extract()  # remove the tags that should be ignored

        lines = list(soup.get_text().split('\n'))
        lines = list(map(lambda x: re.sub(r'[^\x1F-\x7F]+', '', x), lines))
        lines = list(filter(lambda x: x.strip() != "" and any(map(lambda sign: sign in x, TextExtractor.SETENCE_SIGNS)) and x.strip().lower() != host.lower(), lines))

        # print('TextExtractor: ', key, host)

        sentences = []
        for line in lines:
            for sentence in TextExtractor.nlp(line).sents:
                sentences.append(str(sentence))
            
        return sentences

    @staticmethod
    def extract(warc_df, out_file=""):
        sum_cols = udf(TextExtractor.__prepare_payload, ArrayType(StringType()))
        warc_df = warc_df.withColumn(Columns.WARC_CONTENT, sum_cols(Columns.WARC_ID, Columns.WARC_URL, Columns.WARC_CONTENT))
        warc_df = warc_df.withColumn(Columns.WARC_CONTENT, explode(Columns.WARC_CONTENT))
        if out_file != "":
            Writer.csv_writer(out_file, warc_df)
        return warc_df
