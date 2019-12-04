import sys
import pandas as pd
import requests
from collections import namedtuple
import csv

from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe

Hit = namedtuple('Hit', 'text label results')
Result = namedtuple('Result', 'score label id')

INTERESTED_LABELS = [
    'ORG',
    'PERSON',
    'GPE',
]

def query(domain, text, label):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': text, 'size':1000})

    results = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            score = hit.get('_score', 0)

            if freebase_label.lower() == text.lower():
                score *= 10

            results.append(Result(score, freebase_label, freebase_id))

        results.sort(key=lambda x: -x.score)
    
    return Hit(text, label, results)

def main(domain, labelled_file, output_file):
    df = pd.read_csv(labelled_file)

    df['labels'] = df['labels'].apply(lambda x: eval(x))

    with open(output_file, 'w') as out, open(output_file+'.tsv', 'w') as tsv_out:
        out_csv = csv.writer(out)
        out_csv.writerow(['key', 'label', 'freebase_id'])
        for _, row in df.iterrows():
            for text, label in row['labels']:
                if label not in INTERESTED_LABELS: continue

                hit = query(domain, text, label)
                print(row['key'], text, label)
                if len(hit.results) > 0:
                    out_csv.writerow([row['key'], text, hit.results[0].id])
                    tsv_out.write('%s\t%s\t%s\n'%(row['key'],text, hit.results[0].id))
                    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage_and_exit('Usage: python3 %s domain labelled_file output_file'%__file__)

    domain = get_arg(1)
    labelled_file = get_arg(2)
    output_file = get_arg(3)

    main(domain, labelled_file, output_file)