import sys
import pandas as pd
from utils import get_arg, get_arg_from_options, print_usage_and_exit, parallelize_dataframe


def trim_till_different(group, block_size=10):
    if len(group) < 2: return group

    print(group.iloc[0]['host'], end='', flush=True)
    trimTill = 0
    while True:
        lastChar = None
        ok = True
        for _, row in group.iterrows():
            payload = row['payload']

            if len(payload) < trimTill+block_size: 
                ok = False
                break

            segment = payload[trimTill:trimTill+block_size]
            if lastChar is None:
                lastChar = segment
            elif lastChar != segment:
                ok = False
                break

        if not ok:
            break

        trimTill += block_size

    group['payload'] = group['payload'].str.slice(start=trimTill)

    print(" ->",trimTill)

    return group

def main(extraction_file, output_file):
    df = pd.read_csv(extraction_file)

    df_grouped = df.groupby('host')

    df_grouped = df_grouped.apply(trim_till_different)
    
    df_grouped.reset_index().to_csv(output_file, index=False)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage_and_exit('Usage: python3 %s extraction_file output_file '%__file__)

    extraction_file = get_arg(1)
    output_file = get_arg(2)

    main(extraction_file, output_file)