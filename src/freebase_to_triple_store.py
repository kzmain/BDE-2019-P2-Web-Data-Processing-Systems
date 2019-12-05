# requires this command to be run - export PYTHONPATH=/home/jurbani/trident/build-python

import trident

db = trident.Db('/home/jurbani/data/motherkb-trident')

# Define these variables for a given mention.
# freebase_label
# freebase_score
# freebase_id
# freebase_entity_type

# put them in a set
idList.set()
# idList.add(freebase_id)


# facts = {}

def tripleStore(freebase_label, freebase_score, freebase_id, freebase_entity_type):
    for i in idList:
        results = db.sparql("SELECT DISTINCT * WHERE { %s owl:sameas ?o .} LIMIT 100"% freebase_label)
        # TODO:parse trident response
        # TODO:add the count of facts to the dictionary facts[i]= number_of_facts
    # TODO: calculate a score based on freebase_score in relation to fact count
    # TODO: print it.
    
