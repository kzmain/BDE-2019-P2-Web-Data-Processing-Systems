import sys
import pandas as pd
import numpy as np
from multiprocessing import Pool
import textdistance

# Print usage of python file and exit with error code.
def print_usage_and_exit(usage):
    print(usage)
    sys.exit(1)    

def get_arg(index, default = None):
    if index >= len(sys.argv): return default
    return sys.argv[index]

def get_arg_from_options(index, options):
    assert(len(options) > 0)
    option = get_arg(index, options[0])
    if option not in options: option = options[0]
    return option

def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
    

def write_file(filename, text):
    with open(filename, 'w') as f:
        f.write(text)

def tokenize(a):
    return a.split(' ') #TODO: use NLP?

def levenshtein(a, b):
    return textdistance.levenshtein(a, b)

def jaro_winkler(a, b):
    return textdistance.jaro_winkler(a, b)

def jaccard(a , b):
    return textdistance.jaccard(tokenize(a) , tokenize(b))

def sorensen_dice(a , b):
    return textdistance.sorensen_dice(tokenize(a) , tokenize(b))