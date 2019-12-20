# WDPS - Group 1901
This repository contains the Lab Assignment of the 2019 edition of course Web Data Processing of the Master Computer Science at the Vrije Universiteit Amsterdam.

# Application Usage
Working space: '/home/wdps1901/wdps'

-- TODO: Thijmen add commands to run, output files and where to find them and the pipeline flow.
Run:
- Run: `WarcExtractor.py ??`
- Change:

Output:

Configuration:


## Step1: Extraction
This task will extract all usefull information from each web page out of the Web ARChive (WARC).
First the WARCExtractor is ran, found in WarcExtractor.py, which parses each WARC (checks if its valid and contains enough information) and stores the valid WARC output.

This output is processed further by the TextExtractor, found in TextExtractor.py, which uses the Beautiful Soup library to parse the HTML and extract only the useful information.
Next, this extracted information (provided as sentences per parsed web page) is processed by SpacyNLP.py which incorporates the Spacy NLP library. This library extracts Named Entities and provides them for further use to the program.

To summarise, this step performs (1) WARC Extraction (2) HTML Parsing and removing non-interesting data (3) NLP Preprocessing. This will return a set of Named Entities with their respective Named Entity Recognition NER tag.

## Step2: Linking
After the extraction of Named Entities, the entities can be linked to previously found entities in a knowledge base. The actual link step is provided to us by the course in the form of an Elastic Search server. This server is used to link any found Entity and Label combination to an already existing Entity and Label combination in the knowledge base.

The Elastic Search, as provided to us by the course, uses the FreeBase knowledge base. Freebase was a large collaborative knowledge base consisting of data composed mainly by it's community members but has now been shut down (2016) by Google after acquiring the company in 2010.

## Entity Generation


## Entity Ranking


## Unlinkable Mention Prediction


To perform an accurate linking of the found Named Entities and their Labels to the Named Entities and labels from the knowledge base, some ranking of the found links has to be considered. Querying Elastic Search for the found Named Entity and Label combination gives back a series of probable links and their score. Based on this score a first filter is applied, the score must be greater than `6` (chosen as the `THRESHOLD_SCORE`), any link with a lower score is no longer considered and any link that passed this filter is appended to the list of results with an added `DICE_SCORE` which is calculated to be the `Sorensen Dice Score`.

For each link, the hamming score is also calculated and based on a threshold (`> 0.65`) links are further filtered. 
The link ultimately selected as the correct link is the link that:

-- TODO: Thijmen add the last link step, which one is selected?

## Step3: Output
After extracting the entities and linking them to found entities in the Knowledge Base through Elastic Search, the output is generated and written to the output_file.

The output follows the following format: '<WARC_ID>...<NLP_MENTION>...<FREEBASE_ID>'
Where the WARC_ID is the crawled web page ID as provided in the Web ARChive.
Where the NLP_MENTION is the actual found named entity by Spacy.
And where the FREEBASE_ID is the ID as found during the linking phase in the Freebase knowledge base.