from bs4 import BeautifulSoup, Tag, Comment, Doctype, NavigableString
import re 
import os
import sys 
from utils import write_file

TAGS_TO_REMOVE = [
    'head', 'title', 
    'footer', 'header', 
    'noscript', 'script', 
    'style', 'aside',
    '[document]'
]

SETENCE_SIGNS = [
    '.', '?', '!'
]

# Prepare payload by stripping crawled webpages:
    # strip comments and doctype tags,
    # strip undesired tags (TAGS_TO_REMOVE),
    # strip lines that do no pass regex,
    # strip empty words (no chars or space chars),
    # strip sentence signs (SENTENCE_SIGNS),
    # strip any mentions of host
def prepare_payload(key, host, payload):
    soup = BeautifulSoup(payload, 'html5lib')

    dir_name = 'beautiful_soup/%s'%key 
    os.makedirs(dir_name, exist_ok=True)

    write_file('%s/0.raw.html'%dir_name, str(soup))

    for entry in soup.findAll(text=lambda x:isinstance(x, Comment) or isinstance(x, Doctype)):
        entry.extract()
    for tag in soup(TAGS_TO_REMOVE): tag.extract() # remove the tags that should be ignored
    write_file('%s/1.tags_removed.html'%dir_name, str(soup))
    
    lines = list(soup.get_text().split('\n'))
    write_file('%s/2.raw_text.txt'%dir_name, "\n".join(lines))

    lines = list(map(lambda x: re.sub(r'[^\x1F-\x7F]+', '', x), lines))
    lines = list(filter(lambda x: x.strip() != "" and any(map(lambda sign: sign in x, SETENCE_SIGNS)) and x.strip().lower() != host.lower(), lines))

    text = '\n'.join(lines)
    write_file('%s/3.final.txt'%dir_name, text)
    #write_file('%s/3.final2.txt'%dir_name,)

    return lines