import sys
import gzip
import csv
from itertools import starmap
import tqdm
from urllib.parse import urlparse
from collections import namedtuple

from utils import print_usage_and_exit, get_arg_from_options, get_arg

PREPERATION_METHODS = [
    'beautiful_soup'
]

SAMPLE_ONLY = True

HEADER_ID = "WARC-TREC-ID"
HEADER_URI = "WARC-Target-URI"

# Parse WARC record to processable record for extraction
    # parse headers,
    # check if ID and URI present in headers[] array,
    # check if real article (len > 100)
    # return key (ID), parsed URI, payload (html)
def parse_record(raw_record):
    payload_split = raw_record.split('\n\n')

    warc_header = payload_split[0] 
    headers = {}

    # Parse WARC headers with values to only values
    # WARC-Date: 2012-02-10T22:49:56Z
    # headers[WARC-Date] = 2012-02-10T22:49:56Z
    for line in warc_header.splitlines():
        split = line.split(': ')
        headers[split[0]]  = ': '.join(split[1:])
    
    if HEADER_ID in headers and HEADER_URI in headers: 
        key = headers[HEADER_ID]
        uri = headers[HEADER_URI]
        
        payload = '\n\n'.join(payload_split[2:]) # Remove headers
        if len(payload) < 100: return None # Not a real article

        return key, urlparse(uri).netloc, payload
    else: 
        return None

# Read Web ARChive (WARC) file
def get_raw_records(warc_file):
    payload = ''
    with gzip.open(warc_file, "rt", errors="ignore") as stream: 
        for line in stream:
            if line.strip() == "WARC/1.0":
                yield payload
                payload = ''
            else:
                payload += line

# For each WARC record in the Web ARChive: parse record
def get_all_records(warc_file):
    for record in get_raw_records(warc_file):
        record = parse_record(record)

        if record: yield record

def preprocess_record(key, host, payload):
    payload = prepare_payload(key, host, payload)

    return key, host, payload

def main(warc_file, output_file):
    with open(output_file, 'w') as out:
        out_csv = csv.writer(out)
        out_csv.writerow(['key', 'host', 'payload'])
        for key, host, payload in starmap(preprocess_record, get_all_records(warc_file)):
            print(key)

            out_csv.writerow(
                [key, host, payload]
            )
        
            if SAMPLE_ONLY and key == 'clueweb12-0000tw-00-00092': return


if len(sys.argv) < 3:
    print_usage_and_exit("python %s warc_archive_file output_file [preperation_method]"%__file__)

preperation_method = get_arg_from_options(3, PREPERATION_METHODS)
if preperation_method == 'beautiful_soup':
    from preperation.beautiful_soup import prepare_payload

main(get_arg(1), get_arg(2))







