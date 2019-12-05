import spacy
import sys
import validators

nlp = spacy.load("en_core_web_sm")

INTERESTED_LABELS = [
    'ORG',
    'PERSON',
    'GPE',
    'PRODUCT'
]

def special_ratio(text: str):
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
    return has_alpha, m / n 


def generate_entities(payload):
    entries = set()

    for line in payload:
        for ent in nlp(str(line)).ents:
            text = str(ent)
            if(len(text) <= 1): continue

            label = ent.label_
            has_alpha, ratio = special_ratio(text)
            if label in INTERESTED_LABELS:
                if has_alpha and (text[0].isalpha() or text[0].isnumeric()) and not validators.domain(text.lower()) and "  " not in text and ratio < 0.1:
                    entries.add(text)

    return list(entries)