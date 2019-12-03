from bs4 import BeautifulSoup
import re 

IGNORED_TAGS = [
    'head', 'title', 
    'footer', 'header', 
    '[document]', 'script', 
    'style', 'aside'
]

def prepare_payload(payload):
    soup = BeautifulSoup(payload, 'html.parser')

    for tag in soup(IGNORED_TAGS): tag.extract() # remove the tags that should be ignored

    text = " ".join(re.split(r'[\s\n\r]+', soup.get_text().strip())) # remove white spaces and join the words back together 

    return re.sub(r'[^\x1F-\x7F]+', '', text) # remove no ascii stuff