import os
import sys
import uuid

import pandas as pd
from collections import namedtuple
import csv

# from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe
import pyspark
from pyspark.sql.functions import udf, lit, explode, max, min,col
from pyspark.sql.types import StringType, ArrayType

from System import Columns, Location
from Tools import Reader
from Tools.Writer import Writer

import http.client, urllib.parse
import json
import textdistance
import spacy


class Linker:
    THRESHOLD_SCORE = 4
    THRESHOLD_SCORE_H = 0.65
    df_columns = [Columns.LINKER_ENTITY,
                  Columns.LINKER_SCORE,
                  Columns.LINKER_H_SCORE,
                  Columns.LINKER_LABEL,
                  Columns.LINKER_FB_ID]

    SELECT_COUNT = 1
    NO_FREEBASE_LIST = []

    nlp = spacy.load("en_core_web_sm")

    @staticmethod
    def __tokenize(a):
        for word in Linker.nlp(a):
            yield word.text

    @staticmethod
    def __sorensen_dice(a, b):
        return textdistance.sorensen_dice(Linker.__tokenize(a), Linker.__tokenize(b))

    @staticmethod
    def query(domain, query):

        conn = http.client.HTTPConnection(domain.split(':')[0], int(domain.split(':')[1]))
        conn.request('GET', '/freebase/label/_search?%s' % urllib.parse.urlencode({'q': query, 'size': 1000}))
        response = conn.getresponse()

        results = []
        if response.status == 200:
            response = json.loads(response.read())
            for hit in response.get('hits', {}).get('hits', []):
                freebase_label = hit.get('_source', {}).get('label')
                freebase_id = hit.get('_source', {}).get('resource')
                freebase_score = hit.get('_score', 0)

                if freebase_score < Linker.THRESHOLD_SCORE: continue
                dice_score = Linker.__sorensen_dice(query, freebase_label)
                # if dice_score > Linker.THRESHOLD_SCORE_H: continue
                results.append([query, freebase_score, dice_score, freebase_label, freebase_id])
            if len(results) > 0:
                results.sort(key=lambda x: (x[2], -x[1]))
                df = pd.DataFrame(results, columns=Linker.df_columns)
                if not os.path.exists(Location.FREEBASE_PATH_PAR):
                    os.mkdir(Location.FREEBASE_PATH_PAR)
                df.to_parquet(Location.FREEBASE_PATH_PAR + "/" + str(uuid.uuid4()) + ".parquet")
                # results = list(map(lambda x: x[4], results[0:min(len(results), Linker.SELECT_COUNT)]))
            else:
                if query not in Linker.NO_FREEBASE_LIST:
                    Writer.write_list(Location.FREEBASE_PATH_EMP_TXT, [query], "a+")
        elif response.status == 400:
            if query not in Linker.NO_FREEBASE_LIST:
                Writer.write_list(Location.FREEBASE_PATH_EMP_TXT, [query], "a+")
        return results

    @staticmethod
    def link(elastic_domain, trident_domain, spark, nlp_df, out_file=""):
        # Get distinction mentions list from nlp
        mention_list = nlp_df.select(Columns.NLP_MENTION).distinct().toPandas()[Columns.NLP_MENTION]
        mention_list = list(mention_list)
        # mention_list = list(set(Reader.read_txt_to_list("listfile.txt")))
        # Get distinction entities list from stored freebase related info
        try:
            freebase_df = spark.read.parquet(Location.FREEBASE_PATH_PAR + "/*.parquet")
            freebase_list = freebase_df.select(Columns.LINKER_ENTITY).distinct().toPandas()[Columns.LINKER_ENTITY]
            freebase_list = list(set(freebase_list))
        except pyspark.sql.utils.AnalysisException:
            freebase_list = []
        # Get mentions those does not have entities in freebase
        no_entity_list = Reader.read_txt_to_list(Location.FREEBASE_PATH_EMP_TXT)
        Linker.NO_FREEBASE_LIST = no_entity_list
        # Get target list
        target_list = [item for item in mention_list if item not in freebase_list]
        target_list = [item for item in target_list if item not in no_entity_list]

        print("[System]: %s distinct mentions from user." % len(mention_list))
        print("[System]: %s distinct entities from stored parquet." % (len(freebase_list) + len(no_entity_list)))
        print("[System]: %s distinct not processed entities from user." % len(target_list))

        mention_df = spark.createDataFrame(target_list, "string").toDF(Columns.NLP_MENTION)
        sum_cols = udf(Linker.query, ArrayType(StringType()))
        mention_df = mention_df.withColumn(Columns.LINKER_ENTITY, sum_cols(lit(elastic_domain), Columns.NLP_MENTION))
        mention_df.toPandas()

        freebase_df = spark.read.parquet(Location.FREEBASE_PATH_PAR + "/*.parquet")
        ##+--------------------+--------------------+--------------------+-----------------+--------------+----------+----------------+--------+---------+------------------+---------------+----------+
        # |                 key|                host|             payload|           labels|           pos|       tag|             dep| entites|    score|      hamming_core|          label|     fb_id|
        # +--------------------+--------------------+--------------------+-----------------+--------------+----------+----------------+--------+---------+------------------+---------------+----------+
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac| 7.022306|               0.0|       FLIP4MAC| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac|6.8787365|               0.0|       flip4Mac| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac|6.8787365|               0.0|       Flip4mac| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac|6.7670817|               0.0|       Flip4mac| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac|6.8787365|0.6666666666666666|   Flip4Mac WMV| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac|6.7670817|0.6666666666666666|   Flip4Mac WMV| /m/0bf90p|
        # |clueweb12-0000tw-...|centralalbertatv.net|MAC users may nee...|         Flip4Mac|       [PROPN]|     [NNP]|          [oprd]|Flip4Mac| 7.022306|               1.0|       Flip4Mac| /m/0bf90p|

        nlp_df = nlp_df.join(freebase_df, nlp_df.labels == freebase_df.entites, how='full')
        nlp_df = nlp_df.filter(nlp_df.entites.isNotNull())
        score_df = nlp_df.groupBy(col(Columns.NLP_MENTION).alias("n_mention")).agg(min(Columns.LINKER_H_SCORE).alias("h_max"))
        nlp_df = nlp_df.join(score_df, (nlp_df.labels == score_df.n_mention), how='full')
        nlp_df = nlp_df.withColumn("h_checker", col("hamming_core") == col("h_max"))
        nlp_df = nlp_df.filter(col("h_checker"))

        score_df = nlp_df.groupBy(col(Columns.NLP_MENTION).alias("x_mention")).agg(max(Columns.LINKER_SCORE).alias("s_max"))
        nlp_df = nlp_df.join(score_df, (nlp_df.labels == score_df.x_mention), how='full')
        nlp_df = nlp_df.withColumn("s_checker", col("score") == col("s_max"))
        nlp_df = nlp_df.filter(col("s_checker"))

        nlp_df = nlp_df.select([col(Columns.WARC_ID),
                                col(Columns.NLP_MENTION),
                                col(Columns.LINKER_FB_ID).alias(Columns.FREEBASE_ID)])
        nlp_df = nlp_df.dropDuplicates([Columns.WARC_ID, Columns.NLP_MENTION])
        temp_df = nlp_df.toPandas()
        return nlp_df

        # test = mention_df.toPandas()
        # mention_df.show()

# Linker.query("localhost:9200", 'mindenhall/')
# mention_df.show()
# nlp_df = nlp_df.withColumn(Columns.FREEBASE_ID, explode(Columns.LINKER_ENTITY))
# nlp_df = nlp_df.drop(Columns.LINKER_ENTITY)
#
# if out_file != "":
#     Writer.excel_writer(out_file, nlp_df)
#
# return nlp_df

# print(Linker.query("localhost:9200", "France"))
# import os
# import sys
# import uuid
# from functools import partial
# from multiprocessing.pool import Pool
#
# import pandas as pd
# from collections import namedtuple
# import csv
#
# # from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe
# from pyspark.sql.functions import udf, lit, explode
# from pyspark.sql.types import StringType, ArrayType
#
# from System import Columns, Location
# from Tools import Reader
# from Tools.Writer import Writer
#
# import http.client, urllib.parse
# import json
# import textdistance
# import spacy
#
#
# class Linker:
#     THRESHOLD_SCORE = 6
#     THRESHOLD_DICE_SCORE = 0.65
#     SELECT_COUNT = 1
#     df_columns = [Columns.LINKER_ENTITY,
#                   Columns.LINKER_SCORE,
#                   Columns.LINKER_H_SCORE,
#                   Columns.LINKER_LABEL,
#                   Columns.LINKER_FB_ID]
#     nlp = spacy.load("en_core_web_sm")
#
#     @staticmethod
#     def __tokenize(a):
#         for word in Linker.nlp(a):
#             yield word.text
#
#     @staticmethod
#     def __sorensen_dice(a, b):
#         return textdistance.sorensen_dice(Linker.__tokenize(a), Linker.__tokenize(b))
#
#     @staticmethod
#     def query(domain, text):
#         conn = http.client.HTTPConnection(domain.split(':')[0], int(domain.split(':')[1]))
#         conn.request('GET', '/freebase/label/_search?%s' % urllib.parse.urlencode({'q': text, 'size': 1000}))
#         response = conn.getresponse()
#
#         results = []
#         if response.status == 200:
#             response = json.loads(response.read())
#             for hit in response.get('hits', {}).get('hits', []):
#                 freebase_label = hit.get('_source', {}).get('label')
#                 freebase_id = hit.get('_source', {}).get('resource')
#                 freebase_score = hit.get('_score', 0)
#
#                 if freebase_score < Linker.THRESHOLD_SCORE: continue
#                 sorensen_dice = Linker.__sorensen_dice(text, freebase_label)
#                 if sorensen_dice > Linker.THRESHOLD_DICE_SCORE: continue
#                 results.append([text, freebase_score, sorensen_dice, freebase_label, freebase_id])
#             # Ascending order at Hamming Distance, Descending order at freebase score
#             results.sort(key=lambda x: (x[1], x[0]))
#
#             if not os.path.exists(Location.FREEBASE_PATH_PAR):
#                 os.mkdir(Location.FREEBASE_PATH_PAR)
#             df = pd.DataFrame(results, columns=Linker.df_columns)
#             df.to_parquet(Location.FREEBASE_PATH_PAR + "/" + str(uuid.uuid4()) + ".parquet")
#             results = list(map(lambda x: x[3], results[0:min(len(results), Linker.SELECT_COUNT)]))
#         return results
#
#     @staticmethod
#     def link(domain, spark, nlp_df):
#         # Get distinction mentions list from nlp
#         # mention_list = nlp_df.select(Columns.NLP_MENTION).distinct().toPandas()[Columns.NLP_MENTION]
#         # mention_list = list(mention_list)
#         mention_list = Reader.read_txt_to_list("listfile.txt")
#         # Get distinction entities list from stored freebase related info
#         freebase_df = Reader.read_parquet_to_pandas(Location.FREEBASE_PATH_PAR, Linker.df_columns)
#         freebase_list = list(set(freebase_df[Columns.LINKER_ENTITY]))
#         # Get mentions those does not have entities in freebase
#         no_entity_list = Reader.read_txt_to_list(Location.FREEBASE_PATH_EMP_TXT)
#         # Get target list
#         target_list = [item for item in mention_list if item not in freebase_list]
#         target_list = [item for item in target_list if item not in no_entity_list]
#
#         print("[System]: %s distinct mentions from user." % len(mention_list))
#         print("[System]: %s distinct entities from stored parquet." % (len(freebase_list) + len(no_entity_list)))
#         print("[System]: %s distinct not processed entities from user." % len(target_list))
#
#         new_df = spark.createDataFrame(target_list, "string").toDF(Columns.NLP_MENTION)
#         sum_cols = udf(Linker.query, ArrayType(StringType()))
#
#         new_df = new_df.withColumn(Columns.LINKER_ENTITY, sum_cols(lit(domain), Columns.NLP_MENTION))
#
#
#         return nlp_df
#
#     # @staticmethod
#     # def link(freebase_domain, trident_domain, nlp_df):
#     #     df_columns = [Columns.LINKER_ENTITY,
#     #                   Columns.LINKER_SCORE,
#     #                   Columns.LINKER_LABEL,
#     #                   Columns.LINKER_FB_ID]
#     #     # Get distinction mentions list from nlp
#     #     # mention_list = nlp_df.select(Columns.NLP_MENTION).distinct().toPandas()[Columns.NLP_MENTION]
#     #     # mention_list = list(mention_list)
#     #     mention_list = Reader.read_txt_to_list("listfile.txt")
#     #     # Get distinction entities list from stored freebase related info
#     #     freebase_df = Reader.read_parquet_to_pandas(Location.FREEBASE_PATH_PAR, df_columns)
#     #     freebase_list = list(set(freebase_df[Columns.LINKER_ENTITY]))
#     #     # Get mentions those does not have entities in freebase
#     #     no_entity_list = Reader.read_txt_to_list(Location.FREEBASE_PATH_EMP_TXT)
#     #     # Get target list
#     #     target_list = [item for item in mention_list if item not in freebase_list]
#     #     target_list = [item for item in target_list if item not in no_entity_list]
#     #
#     #     print("[System]: %s distinct mentions from user." % len(mention_list))
#     #     print("[System]: %s distinct entities from stored parquet." % (len(freebase_list) + len(no_entity_list)))
#     #     print("[System]: %s distinct not processed entities from user." % len(target_list))
#     #
#     #     n_df = pd.DataFrame([], columns=df_columns)
#     #     empty_list = []
#     #     try:
#     #         for mention in target_list:
#     #
#     #             re_list = Linker.query(freebase_domain, mention)
#     #             if len(re_list) == 0:
#     #                 empty_list.append(mention)
#     #                 continue
#     #             temp_df = pd.DataFrame(re_list, columns=df_columns)
#     #
#     #             # for index, row in temp_df.iterrows():
#     #             #     get = Linker.sparql(trident_domain, row[Columns.LINKER_FB_ID].replace("/m/", "m."))
#     #
#     #             n_df = n_df.append(temp_df, ignore_index=True)
#     #     except OSError:
#     #         print("[ERROR]: Elastic server failed")
#     #
#     #     # Processed finish write out
#     #     if not os.path.exists(Location.FREEBASE_PATH_PAR):
#     #         os.mkdir(Location.FREEBASE_PATH_PAR)
#     #     if len(n_df) > 0:
#     #         n_df.to_parquet(Location.FREEBASE_PATH_PAR + "/" + str(uuid.uuid4()) + ".parquet")
#     #         print("[System]: Processed %s entities stored." % len(n_df))
#     #     Writer.write_list(Location.FREEBASE_PATH_EMP_TXT, empty_list, "a+")
#     #
#     #     return nlp_df
#
# # print(Linker.query("localhost:12306", "France"))
