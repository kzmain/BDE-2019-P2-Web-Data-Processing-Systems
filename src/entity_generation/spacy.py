import spacy
import sys
import validators

nlp = spacy.load("en_core_web_sm")
stops = spacy.lang.en.stop_words.STOP_WORDS

def normalize(text):
    text = nlp(text)
    lemmatized = list()
    for word in text:
        lemma = word.lemma_.strip()
        if lemma:
            if lemma not in stops:
                lemmatized.append(lemma)
    return " ".join(lemmatized)


INTERESTED_LABELS = [
    'ORG',
    'PERSON',
    'GPE',
    'PRODUCT'
]

def special_ratio(text: str):
    m = 0
    n = len(text)
    for i in range(n):
        c = text[i]
        if c == ' ' or c == '.' or c.isnumeric() or c.isalpha(): continue
        m += 1
    return m / n 


def generate_entities(payload):

    entries = set()


    for line in payload:
        for ent in nlp(str(line)).ents:

            text = str(ent)
            label = ent.label_
            ratio = special_ratio(text)
            if label in INTERESTED_LABELS:
                if not validators.domain(text.lower()) and "  " not in text and ratio < 0.1:
                    entries.add(text)

    return list(entries)