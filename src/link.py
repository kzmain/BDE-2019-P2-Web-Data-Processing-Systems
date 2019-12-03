import sys
import pandas as pd
import requests

from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe

def query(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size':1000})
    id_labels = {}

    if response:
        response = response.json()

        #print(response)
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            id_labels.setdefault(freebase_id, set()).add( freebase_label )

    return id_labels

def main(domain, labelled_file, output_file):
    df = pd.read_csv(labelled_file)

    df['labels'] = df['labels'].apply(lambda x: eval(x))

    for _, row in df.iterrows():
        for text, label in row['labels']:
            print(text, label)
            
            response = query(domain, text)

            if response:
                return
    
    df.to_csv(output_file, index=False)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage_and_exit('Usage: python3 %s domain labelled_file output_file'%__file__)

    domain = get_arg(1)
    labelled_file = get_arg(2)
    output_file = get_arg(3)

    main(domain, labelled_file, output_file)