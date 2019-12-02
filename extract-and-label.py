import gzip
import sys

from preperation.beautiful_soup import prepare_payload


ENTITY_GEN_TOOL = 'spacy'
if len(sys.argv) > 3:
    ENTITY_GEN_TOOL = sys.argv[3] 

if ENTITY_GEN_TOOL == 'spacy':
    from entity_generation.spacy import generate_entities
elif ENTITY_GEN_TOOL == 'stanford_nlp':
    from entity_generation.stanford_nlp import generate_entities

print('using %s...'%ENTITY_GEN_TOOL)

KEYNAME = "WARC-TREC-ID"

class Record:
    def __init__(self, key, payload):
        self.key = key
        self.payload = payload

    def __str__(self):
        return "%s (length: %s)"%(self.key, len(self.payload))

def parse_record(raw_record) -> Record:
    key = None
    for line in raw_record.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    if key: 
        return Record(key, '\n\n'.join(raw_record.split('\n\n')[2:])) # Remove headers
    else: 
        return None

def get_raw_records(warc_file):
    payload = ''
    with gzip.open(warc_file, "rt", errors="ignore") as stream: 
        for line in stream:
            if line.strip() == "WARC/1.0":
                yield payload
                payload = ''
            else:
                payload += line

def get_all_records(warc_file):
    for record in get_raw_records(warc_file):
        record = parse_record(record)

        if record: yield record

def preprocess_record(record: Record):
    record.payload = prepare_payload(record.payload)

    return record

def main(input_file, output_file):
    for record in map(preprocess_record, get_all_records(input_file)):
        for text, label in generate_entities(record.payload):
            print(record.key, text, label)

def print_usage_and_exit():
    print('Usage: python3 extract.py INPUT OUTPUT')
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage_and_exit()

    main(sys.argv[1], sys.argv[2])