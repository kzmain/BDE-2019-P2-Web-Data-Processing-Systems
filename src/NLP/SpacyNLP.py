from array import ArrayType

import spacy
import validators

# from abc import ABC

from pyspark.sql.functions import udf, col, size, explode
from pyspark.sql.types import StringType, ArrayType

from System import Columns
from Tools.Writer import Writer

class SpacyNLP:
    nlp = spacy.load("en_core_web_sm")

    INTERESTED_LABELS = [
        'ORG',
        'PERSON',
        'GPE',
        'PRODUCT'
    ]
    
    # Calculate special characters (non-alphabet) to alphabet ratio
    @staticmethod
    def __special_ratio(text: str):
        m = 0
        n = len(text)
        has_alpha = False
        for i in range(n):
            c = text[i]
            if c == ' ' or c == '.': continue
            if c.isnumeric(): continue
            if c.isalpha():
                has_alpha = True
                continue

            m += 1
        return has_alpha, m / n # Return if text has alphabetical text and ratio as tuple


    @staticmethod
    def __generate_entities(key, host, payload):
        #print("SpacyNLP: %s"%key)
        entries = []
        for token in SpacyNLP.nlp(str(payload)):
            if token.ent_type_ in SpacyNLP.INTERESTED_LABELS:
                text = str(token)
                has_alpha, ratio = SpacyNLP.__special_ratio(text)
                if has_alpha and (text[0].isalpha() or text[0].isnumeric()) and not validators.domain(text.lower()) and "  " not in text and ratio < 0.1:
                    entries.append([text, token.pos_, token.tag_, token.dep_])
        return entries

    @staticmethod
    def extract(text_df, out_file=""):
        sum_cols = udf(SpacyNLP.__generate_entities, ArrayType(ArrayType(StringType())))

        text_df = text_df.withColumn(Columns.NLP_NLP, sum_cols(Columns.WARC_ID, Columns.WARC_URL, Columns.WARC_CONTENT))
        text_df = text_df.withColumn(Columns.NLP_SIZE, size(col(Columns.NLP_NLP)))
        text_df = text_df.filter(col(Columns.NLP_SIZE) >= 1)
        text_df = text_df.withColumn(Columns.NLP_NLP, explode(Columns.NLP_NLP))
        text_df = text_df.withColumn(Columns.NLP_MENTION, col(Columns.NLP_NLP).getItem(0))\
            .withColumn(Columns.NLP_POS, col(Columns.NLP_NLP).getItem(1))\
            .withColumn(Columns.NLP_TAG, col(Columns.NLP_NLP).getItem(2))\
            .withColumn(Columns.NLP_DEP, col(Columns.NLP_NLP).getItem(3))
        text_df = text_df.drop(col(Columns.NLP_NLP))
        text_df = text_df.drop(col(Columns.NLP_SIZE))
        text_df = text_df.drop(col(Columns.WARC_CONTENT))
        if out_file != "":
            Writer.excel_writer(out_file, text_df)
        return text_df

