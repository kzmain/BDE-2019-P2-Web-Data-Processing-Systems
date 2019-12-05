import sys

import pandas as pd
import requests
from collections import namedtuple
import csv

# from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType, ArrayType

from System import Columns


class Linker:
    Hit = namedtuple('Hit', 'text results')
    Result = namedtuple('Result', 'score label id')

    THRESHOLD_SCORE = 6

    @staticmethod
    def query(domain, text):
        url = 'http://%s/freebase/label/_search' % domain
        response = requests.get(url, params={'q': text, 'size': 1000})

        results = []
        if response:
            response = response.json()
            for hit in response.get('hits', {}).get('hits', []):
                freebase_label = hit.get('_source', {}).get('label')
                freebase_id = hit.get('_source', {}).get('resource')
                score = hit.get('_score', 0)

                if freebase_label.lower() == text.lower():
                    score *= 10

                if score >= Linker.THRESHOLD_SCORE:
                    results.append([str(score), str(freebase_label), str(freebase_id)])

            # results.sort(key=lambda x: -x.score)

        return [[]]

    @staticmethod
    def link(domain, nlp_df, output_file):
        sum_cols = udf(Linker.query, ArrayType(ArrayType(StringType())))
        nlp_df = nlp_df.withColumn(Columns.LINKER_ENTITY, sum_cols(lit(domain), Columns.NLP_MENTION))
        nlp_df.show()
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
