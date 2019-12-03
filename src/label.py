import sys
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe

KEYNAME = "WARC-TREC-ID"

GENERATION_METHODS = [
    'spacy',
    'stanford_nlp'
]

def generate_labels(df):
    df['labels'] = df['payload'].progress_apply(lambda x: list(generate_entities(x)))   
    
    return df[['key', 'labels']]


def main(extraction_file, output_file):
    df = pd.read_csv(extraction_file)

    df = parallelize_dataframe(df, generate_labels)
    
    df.to_csv(output_file, index=False)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage_and_exit('Usage: python3 %s extraction_file output_file <generation_method>'%__file__)

    extraction_file = get_arg(1)
    output_file = get_arg(2)

    generation_method = get_arg_from_options(4, GENERATION_METHODS)
    if generation_method == 'spacy':
        from entity_generation.spacy import generate_entities
    elif generation_method == 'stanford_nlp':
        from entity_generation.stanford_nlp import generate_entities

    main(extraction_file, output_file)