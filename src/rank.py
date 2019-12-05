import textdistance
import pandas as pd
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from tqdm import tqdm
tqdm.pandas()
import os

from utils import *

THRESHOLD_SCORE = 6

BENCHMARK = False
COMPUTE_SCORE = True

DISTANCE_ALGORITHMS = [
    levenshtein,
    jaro_winkler,
    jaccard,
    sorensen_dice
]
DISTANCE_ALGORITHM = sorensen_dice
SELECT_COUNT = 1

# Process each group (grouped by key, text combination)
    # if length < 1, do not consider group
    # only consider SELECT_COUNT rows from group (default: 1 (considering only top record))
def process_group(group, out):
    if len(group) < 1: return
    key = group.iloc[0]['key']
    text = group.iloc[0]['text']
    # print(key, text)

    i = 0
    for _, row in group.iterrows():
        out.write('%s\t%s\t%s\n'%(key, text, row['freebase_id']))
        i += 1

        if i >= SELECT_COUNT: break
    
# Read and filter linked_file (output of link.py)
    # filter for scores above threshold so only highly probable relevant links are considered
    # calculate distance based on selected algorithm, default: sorensen_dice
    # write each group to output_file as defined in process_group()
def main(linked_file, output_file):
    df = pd.read_csv(linked_file)
    df = df[df['score'] > THRESHOLD_SCORE]

    print('computing distance...')
    df['distance'] = df.progress_apply(lambda x: DISTANCE_ALGORITHM(x['text'], x['label']), axis=1)
    df = df.sort_values(['distance', 'score'], ascending=[True, False])

    grouped = df.groupby(['key', 'text'])

    print('selecting entities...')
    with open(output_file, 'w') as out:
        grouped.progress_apply(lambda group: process_group(group, out))

# After running, if score is to be calculated (COMPUTER_SCORE = True (default: True))
    # calculate score by running score.py by comparing output_file to sample.annotations.tsv (control file)                   
def run():
    main(linked_file, output_file)

    if COMPUTE_SCORE: 
        os.system('python ../score.py ../data/sample.annotations.tsv %s'%output_file)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print_usage_and_exit('Usage: python3 %s linked_file output_file'%__file__)

    linked_file = get_arg(1)
    output_file = get_arg(2)

    if BENCHMARK:
        for algo in DISTANCE_ALGORITHMS:
            DISTANCE_ALGORITHM = algo
            print("testing %s..."%str(DISTANCE_ALGORITHM))

            run()
    else: run()


