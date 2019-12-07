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
    def sparql(domain, fb_id):
        # url = 'http://%s/sparql' % domain
        # response = requests.post(url, data={'print': True, 'query': query})
        query = "select distinct ?abstract where {  \
          ?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/%s> .  \
          ?s <http://www.w3.org/2002/07/owl#sameAs> ?o . \
          ?o <http://dbpedia.org/ontology/abstract> ?abstract . \
        }" % fb_id

        conn = http.client.HTTPConnection(domain.split(':')[0], int(domain.split(':')[1]))
        conn.request('POST', '/sparql?%s' % urllib.parse.urlencode({'print': True, 'query': query}))
        response = conn.getresponse()
        if response:
            try:
                response = response.json()
                print(json.dumps(response, indent=2))
            except Exception as e:
                print(e)
                raise e

    @staticmethod
    def query(elastic_domain, trident_domain, query):

        conn = http.client.HTTPConnection(elastic_domain.split(':')[0], int(elastic_domain.split(':')[1]))
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
                if dice_score > Linker.THRESHOLD_SCORE_H: continue

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
        mention_df = mention_df.withColumn(Columns.LINKER_ENTITY, sum_cols(lit(elastic_domain),lit(trident_domain), Columns.NLP_MENTION))
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

        return nlp_df
