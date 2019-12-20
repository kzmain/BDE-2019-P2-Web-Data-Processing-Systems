import requests

###     =============================================================================
###     |                                                                           |
###     |   This is the Elastic Search independent file                             |
###     |       It is responsible for offering an easy way of quickly querying      |
###     |       the Elastic Search server for testing purposes.                     |
###     |                                                                           |
###     =============================================================================


# Query elastic search for entity query
    # used if all results stored by link.py and rank.py are too big since sample gets too big,
    # for small sample: use link.py and rank.py to store entity results beforehand for faster processing
def search(domain, query):
    # Build URL and query the server
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size':1000})

    # If the response is non-empty, return the ID_Labels from this function.
    id_labels = {}
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            id_labels.setdefault(freebase_id, set()).add( freebase_label )

    return id_labels

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python elasticsearch.py DOMAIN QUERY')
        sys.exit(0)

    for entity, labels in search(DOMAIN, QUERY).items():
        print(entity, labels)
