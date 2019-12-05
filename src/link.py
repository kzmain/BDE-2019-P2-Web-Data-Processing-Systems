import sys
import pandas as pd
import requests
from collections import namedtuple
import csv
import textdistance

from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe

Hit = namedtuple('Hit', 'text results')
Result = namedtuple('Result', 'score label id')

def query(domain, text):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': text, 'size':1000})

    results = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            freebase_score = hit.get('_score', 0)
            
            results.append(Result(freebase_score, freebase_label, freebase_id))

        results.sort(key=lambda x: -x.score)
    
    return Hit(text, results)

def main(domain, labelled_file, output_file):
    df = pd.read_csv(labelled_file)

    df['labels'] = df['labels'].apply(lambda x: eval(x))

    with open(output_file, 'w') as out:
        out_csv = csv.writer(out)
        out_csv.writerow(['key', 'text', 'label', 'score', 'freebase_id'])
        for _, row in df.iterrows():
            for text in row['labels']:
                print(row['key'], text)

                hit = query(domain, text)
                for result in hit.results:
                    out_csv.writerow([row['key'], text, result.label, result.score, result.id])
                    

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print_usage_and_exit('Usage: python3 %s domain labelled_file output_file'%__file__)

    domain = get_arg(1)
    labelled_file = get_arg(2)
    output_file = get_arg(3)

    main(domain, labelled_file, output_file)