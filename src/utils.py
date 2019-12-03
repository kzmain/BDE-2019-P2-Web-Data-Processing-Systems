import sys
import pandas as pd
import numpy as np
from multiprocessing import Pool

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