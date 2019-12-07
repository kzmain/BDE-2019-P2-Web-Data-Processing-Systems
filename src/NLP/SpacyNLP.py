from array import ArrayType

import spacy
import validators

from pyspark.sql.functions import udf, col, size, explode
from pyspark.sql.types import StringType, ArrayType

from System import Columns as Col
from Tools.Writer import Writer


class SpacyNLP:
    nlp = spacy.load("en_core_web_sm")

    INTERESTED_LABELS = [
        'ORG',
        'PERSON',
        'GPE',
        'PRODUCT'
    ]

    TOKEN_POS = "pos"
    TOKEN_TAG = "tag"
    TOKEN_DEP = "dep"

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
        return has_alpha, m / n  # Return if text has alphabetical text and ratio as tuple

    # @staticmethod
    # def generate_entities(payload):
    #     entries = []
    #     for token in SpacyNLP.nlp(str(payload)):
    #         if token.ent_type_ in SpacyNLP.INTERESTED_LABELS:
    #             token_txt = str(token)
    #             ratio = SpacyNLP.__special_ratio(token_txt)
    #             if not validators.domain(token_txt.lower()) and "  " not in token_txt and ratio < 0.1:
    #                 entries.append([token_txt, token.pos_, token.tag_, token.dep_])
    #
    #     return entries

    @staticmethod
    def __get_token_dict(nlp):

        token_dict = {}
        for token in nlp:
            token_dict[str(token)] = {SpacyNLP.TOKEN_POS: token.pos_,
                                      SpacyNLP.TOKEN_TAG: token.tag_,
                                      SpacyNLP.TOKEN_DEP: token.dep_}
        return token_dict

    @staticmethod
    def __confirm_filter(ent):
        ent = str(ent)
        has_alpha, ratio = SpacyNLP.__special_ratio(ent)
        if has_alpha \
                and (ent[0].isalpha() or ent[0].isnumeric()) \
                and not validators.domain(ent.lower()) \
                and "  " not in ent \
                and ratio < 0.1:
            return True
        else:
            return False

    @staticmethod
    def __pack_entity(ent, token_dict):
        ent = str(ent)
        pos_list = []
        tag_list = []
        dep_list = []
        for token in ent.split(" "):
            try:
                pos_list.append(token_dict[token][SpacyNLP.TOKEN_POS])
                tag_list.append(token_dict[token][SpacyNLP.TOKEN_TAG])
                dep_list.append(token_dict[token][SpacyNLP.TOKEN_DEP])
            except KeyError:
                pass
        return [ent, pos_list, tag_list, dep_list]

    @staticmethod
    def generate_entities(key, payload):
        print("SpacyNLP: %s" % key)
        entries = []
        ent_set = set()
        nlp = SpacyNLP.nlp(payload)
        token_dict = SpacyNLP.__get_token_dict(nlp)

        for ent in nlp.ents:
            if ent.label_ in SpacyNLP.INTERESTED_LABELS and SpacyNLP.__confirm_filter(ent) and str(ent) not in ent_set:
                entries.append(SpacyNLP.__pack_entity(ent, token_dict))
                ent_set.add(str(ent))
        return entries

    @staticmethod
    def extract(text_df, out_file=""):
        sum_cols = udf(SpacyNLP.generate_entities, ArrayType(ArrayType(StringType())))
        text_df = text_df.withColumn(Col.NLP_NLP, sum_cols(Col.WARC_ID, Col.WARC_CONTENT))
        text_df = text_df.withColumn(Col.NLP_SIZE, size(col(Col.NLP_NLP)))
        text_df = text_df.filter(col(Col.NLP_SIZE) >= 1)
        text_df = text_df.withColumn(Col.NLP_NLP, explode(Col.NLP_NLP))
        text_df = text_df.withColumn(Col.NLP_MENTION, col(Col.NLP_NLP).getItem(0)) \
            .withColumn(Col.NLP_POS, col(Col.NLP_NLP).getItem(1)) \
            .withColumn(Col.NLP_TAG, col(Col.NLP_NLP).getItem(2)) \
            .withColumn(Col.NLP_DEP, col(Col.NLP_NLP).getItem(3))
        text_df = text_df.drop(col(Col.NLP_NLP))
        text_df = text_df.drop(col(Col.NLP_SIZE))
        if out_file != "":
            Writer.csv_writer(out_file, text_df)
        return text_df

    # @staticmethod
    # def generate_entities(key, host, payload):
    #     print("SpacyNLP: %s" % key)
    #
    #     entries = []
    #     for line in payload:
    #         ents = SpacyNLP.nlp(str(line)).ents
    #         for ent in ents:
    #             if ent.label_ in SpacyNLP.INTERESTED_LABELS:
    #                 text = str(ent)
    #                 has_alpha, ratio = SpacyNLP.__special_ratio(text)
    #                 if has_alpha and (text[0].isalpha() or text[0].isnumeric()) and not validators.domain(
    #                         text.lower()) and "  " not in text and ratio < 0.1:
    #                     if text not in entries: entries.append(text)
    #     return entries
    #
    # @staticmethod
    # def extract(text_df, out_file=""):
    #     sum_cols = udf(SpacyNLP.generate_entities, ArrayType(StringType()))
    #
    #     text_df = text_df.withColumn(Col.NLP_MENTION, sum_cols(Col.WARC_ID, Col.WARC_URL, Col.WARC_CONTENT))
    #     text_df = text_df.drop(col(Col.WARC_CONTENT))
    #     text_df = text_df.drop(col(Col.WARC_URL))
    #
    #     text_df = text_df.withColumn(Col.NLP_SIZE, size(col(Col.NLP_MENTION)))
    #     text_df = text_df.filter(col(Col.NLP_SIZE) >= 1)
    #     text_df = text_df.withColumn(Col.NLP_MENTION, explode(Col.NLP_MENTION))
    #     # text_df = text_df.withColumn(Columns.NLP_MENTION, col(Columns.NLP_NLP).getItem(0))#\
    #     # .withColumn(Columns.NLP_POS, col(Columns.NLP_NLP).getItem(1))\
    #     # .withColumn(Columns.NLP_TAG, col(Columns.NLP_NLP).getItem(2))\
    #     # .withColumn(Columns.NLP_DEP, col(Columns.NLP_NLP).getItem(3))
    #     # text_df = text_df.drop(col(Columns.NLP_NLP))
    #     text_df = text_df.drop(col(Col.NLP_SIZE))
    #
    #     if out_file != "":
    #         Writer.excel_writer(out_file, text_df)
    #     return text_df

#
# pay_load = """Google Analytics is a web analytics service offered by Google that tracks and reports website traffic, currently as a platform inside the Google Marketing Platform brand.[1] Google launched the service in November 2005 after acquiring Urchin.[2][3]
# As of 2019, Google Analytics was the most widely used web analytics service on the web.[4] Google Analytics provides an SDK that allows gathering usage data from iOS and Android app, known as Google Analytics for Mobile Apps.[5] Google Analytics can be blocked by browsers, browser extensions, and firewalls.
# """
# SpacyNLP.generate_entities("", pay_load)
