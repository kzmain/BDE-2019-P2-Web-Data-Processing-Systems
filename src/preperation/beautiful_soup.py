from bs4 import BeautifulSoup
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

def prepare_payload(key, payload):
    soup = BeautifulSoup(payload, 'html5lib')

    dir_name = 'beautiful_soup/%s'%key 
    os.makedirs(dir_name, exist_ok=True)


    write_file('%s/0.raw.html'%dir_name, str(soup))
    for tag in soup(TAGS_TO_REMOVE): tag.extract() # remove the tags that should be ignored
    write_file('%s/1.tags_removed.html'%dir_name, str(soup))

    text = soup.get_text().strip()
    write_file('%s/2.raw_text.txt'%dir_name, text)
    lines = text.split('\n') 
    lines = filter(lambda x: x.strip() != "", lines)
    text = ' '.join(lines)

    write_file('%s/3.final.txt'%dir_name, text)

    return text