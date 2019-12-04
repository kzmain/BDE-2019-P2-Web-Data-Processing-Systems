from bs4 import BeautifulSoup
import re 
import sys 
from utils import write_file

TAGS_TO_REMOVE = [
    'head', 'title', 
    'footer', 'header', 
    'noscript', 'script', 
    'style', 'aside'
]

def prepare_payload(payload):
    soup = BeautifulSoup(payload, 'html.parser')

    for tag in soup(TAGS_TO_REMOVE): tag.extract() # remove the tags that should be ignored

    text = soup.get_text().strip()
    text = re.sub(r'(\n\s*)+', '\n', text) # replace n newlines with just 1 newline

    lines = text.split('\n') 
    lines = map(
        lambda x: x + '.' if not x[-1] == '.' else x, # add a dot after each sentence
            filter(lambda x: x != "", # filter out the empty strings
                map(lambda x: re.sub(r'[^\x1F-\x7F]+', '', x).strip(), lines) # strip each sentence of white spaces and replace non ascii chars
            )
        )
    text = ' '.join(lines)

    return text