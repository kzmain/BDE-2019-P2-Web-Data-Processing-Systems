import gzip
import re
import sys
from bs4 import BeautifulSoup

KEYNAME = "WARC-TREC-ID"
import nlp_lib

def find_labels(payload, labels):
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    for label, freebase_id in labels.items():
        if key and (label in payload):
            yield key, label, freebase_id


def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line


if __name__ == '__main__':
    if len(sys.argv) < 1:
        print('Usage: python3 starter-code.py INPUT')
        sys.exit(0)

    cheats = dict((line.split('\t', 2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
    warcfile = gzip.open(sys.argv[1], "rt", errors="ignore")
    for record in split_records(warcfile):
        warc_info = re.search(r"WARC-Type((?!Content-Length)(\n|.))*(\n).*", record)
        warc_info = record[warc_info.start(): warc_info.end()] if warc_info is not None else ""

        http_info = re.search(r"HTTP((?!Content-Type)(\n|.))*(\n).*", record)
        http_info = record[http_info.start(): http_info.end()] if http_info is not None else ""
        #
        record = record.replace(warc_info, "")
        record = record.replace(http_info, "")
        #
        record = re.sub(r"<script((?!>)(\n|.))*>((?!<\/)(\w|\W))*<\/script>", "", record)
        record = re.sub(r"<style((?!>)(\n|.))*>((?!<\/)(\w|\W))*<\/style>", "", record)
        record = re.sub(r"<((?!\>)(\n|.))+>", "", record)
        #
        record = re.sub(r"^(\t)+", "", record, flags=re.MULTILINE)
        record = re.sub(r"^( )+", "", record, flags=re.MULTILINE)
        record = re.sub(r"^(\n)+", "", record, flags=re.MULTILINE)
        #
        record = record.encode('ascii', "ignore").decode()

        nlp = nlp_lib.StanfordNLP()
        result_dict = nlp.annotate(record)
        print(result_dict)

        # for key, label, freebase_id in find_labels(record, cheats):
        #     print(key + '\t' + label + '\t' + freebase_id)
