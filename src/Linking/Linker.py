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
from pyspark.sql import DataFrame

from System import Columns, Location
from Tools import Reader
from Tools.Writer import Writer

import http.client, urllib.parse
import json
import textdistance
import spacy

###     ============================================================================
###     |                                                                          |
###     |   This is the Linker class                                               |
###     |       It is responsible for generating candidate entities as found       |
###     |       by the Elastic Search server by querying for each Named Entity     |
###     |       as found by the NLP Preprocessing task. It ranks the candidates    |
###     |       and selects the most probable link (highest rank) and finalises.   |
###     |                                                                          |
###     ============================================================================

class Linker:
###     =========================
###     |   Declare constants   |
###     =========================

    # Theshold on freebase_score as provided by the  
    # FreeBase instance behind the Elastic Search server.
    THRESHOLD_SCORE = 6

    # Load the small (web_sm) Spacy package for the English language
    # Instead of the large (web_lg) as it gave a non-significant 
    # improvement but increased execution time significantly
    nlp = spacy.load("en_core_web_sm")

    # Tokenize each sentence into words
    @staticmethod
    def __tokenize(a):
        for word in Linker.nlp(a):
            yield word.text

    # Calculate the Sorensen Dice Score for two entities.
    @staticmethod
    def __sorensen_dice(a, b):
        return textdistance.sorensen_dice(Linker.__tokenize(a), Linker.__tokenize(b))

    # UNUSED AS IT CURRENTLY STANDS (gave no significant improvement of F1-Score)
    # Query the provided trident with SPARQL with the Named Entities found
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

    #   Query the Elastic Search server instance with the found Named Entities
    #       - Retrieve the Candidate Entities   (Candidate Entity Generation)
    #       - Rank the Candidate Entities       (Candidate Entity Ranking)      >> Automatically done (freebase_score)
    #           - Remove unlinkable Entities    (Unlinkable Mention Prediction)
    #       
    #       -- Return a list of Named Entities with their probable links (Candidate Entities)
    @staticmethod
    def query(domain, key, text):
        conn = http.client.HTTPConnection(domain.split(':')[0], int(domain.split(':')[1]))
        conn.request('GET', '/freebase/label/_search?%s'%urllib.parse.urlencode({'q': text, 'size': 1000}))
        response = conn.getresponse()
        
        results = []
        if response.status == 200:
            print("Linker: ", key, text)
            response = json.loads(response.read())
            for hit in response.get('hits', {}).get('hits', []):
                freebase_label = hit.get('_source', {}).get('label')
                freebase_id = hit.get('_source', {}).get('resource')
                freebase_score = hit.get('_score', 0)
                
                if freebase_score < Linker.THRESHOLD_SCORE: continue
                results.append([freebase_score, Linker.__sorensen_dice(text, freebase_label), freebase_label, freebase_id])
            
            if len(results) > 0:
                results.sort(key=lambda x: (x[1], x[0]))
                return results[0][3]

        return ""

    @staticmethod
    def link(es_domain, trident_domain, nlp_df: DataFrame, out_file=""):
        sum_cols = udf(Linker.query, StringType())
        nlp_df = nlp_df.withColumn(Columns.FREEBASE_ID, sum_cols(lit(es_domain), Columns.WARC_ID, Columns.NLP_MENTION))
        nlp_df = nlp_df.where((col(Columns.FREEBASE_ID) != ""))
       
        if out_file != "":
            Writer.csv_writer(out_file, nlp_df)

        return nlp_df
