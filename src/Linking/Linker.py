import sys

import pandas as pd
from collections import namedtuple
import csv

# from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe
from pyspark.sql.functions import udf, lit, explode
from pyspark.sql.types import StringType, ArrayType

from System import Columns

import http.client, urllib.parse
import json
import textdistance
import spacy


class Linker:
    THRESHOLD_SCORE = 6
    SELECT_COUNT = 1

    nlp = spacy.load("en_core_web_sm")

    @staticmethod
    def tokenize(a):
        for word in Linker.nlp(a):
            yield word.text

    @staticmethod
    def sorensen_dice(a , b):
        return textdistance.sorensen_dice(Linker.tokenize(a) , Linker.tokenize(b))

    @staticmethod
    def query(domain, text):
        
        conn = http.client.HTTPConnection(domain.split(':')[0], int(domain.split(':')[1]))
        conn.request('GET', '/freebase/label/_search?%s'%urllib.parse.urlencode({'q': text, 'size': 1000}))
        response = conn.getresponse()

        results = []
        if response.status == 200:
            response = json.loads(response.read())
            for hit in response.get('hits', {}).get('hits', []):
                freebase_label = hit.get('_source', {}).get('label')
                freebase_id = hit.get('_source', {}).get('resource')
                freebase_score = hit.get('_score', 0)
                
                if freebase_score < Linker.THRESHOLD_SCORE: continue
                results.append([freebase_score, Linker.sorensen_dice(text, freebase_label), freebase_label, freebase_id])
            
            results.sort(key=lambda x: (-x[0], x[1]))
            results = list(map(lambda x: x[3], results[0:min(len(results), Linker.SELECT_COUNT)]))

        return results

    @staticmethod
    def link(domain, nlp_df, output_file):
        sum_cols = udf(Linker.query, ArrayType(StringType()))
        nlp_df = nlp_df.withColumn(Columns.LINKER_ENTITY, sum_cols(lit(domain), Columns.NLP_MENTION))
        nlp_df = nlp_df.withColumn(Columns.FREEBASE_ID, explode(Columns.LINKER_ENTITY))
        nlp_df = nlp_df.drop(Columns.LINKER_ENTITY)
        return nlp_df

# if __name__ == '__main__':
#     if len(sys.argv) < 2:
#         print_usage_and_exit('Usage: python3 %s domain labelled_file output_file' % __file__)
#
#     domain = get_arg(1)
#     labelled_file = get_arg(2)
#     output_file = get_arg(3)
#
#     main(domain, labelled_file, output_file)
