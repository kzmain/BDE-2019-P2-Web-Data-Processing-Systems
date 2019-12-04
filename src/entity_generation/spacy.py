import spacy

nlp = spacy.load("en_core_web_sm")

def generate_entities(payload):
    doc = nlp(payload)

    for ent in doc.ents:
        yield(ent.text, ent.label_)

    # document = nlp(payload)

    # for x in document.sents:
    #     [(x.orth_,x.pos_, x.lemma_) for x in [y 
    #                                   for y
    #                                   in nlp(str(x)) 
    #                                   if not y.is_stop and y.pos_ != 'PUNCT']]
    #     z = dict([(str(x), x.label_) for x in nlp(str(x)).ents])
    #     for text, label in z.items():
    #         yield (text,label)