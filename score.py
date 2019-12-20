import sys
import os 

###     ==================================================================
###     |                                                                |
###     |   This file is responsible for easily calculating the scores   |
###     |       of the retrieved output from the NER process             |
###     |                                                                |
###     ==================================================================


gold_file = sys.argv[1]
pred_file = sys.argv[2]

if pred_file.startswith("hdfs"):
    intermediate_file = 'hdfs_results.tsv'
    os.system('hadoop fs -get -f %s %s' %(pred_file, intermediate_file))
    pred_file = intermediate_file

# Load the gold standard
gold = {}
for line in open(gold_file):
    record, string, entity = line.strip().split('\t', 2)
    gold[(record, string)] = entity
n_gold = len(gold)
print('Gold entities: %s' % n_gold)

# Load the predictions
pred = {}
for line in open(pred_file):
    record, string, entity = line.strip().split('\t', 2)
    pred[(record, string)] = entity
n_predicted = len(pred)
print('Linked entities: %s' % n_predicted)

# Evaluate predictions
n_correct = sum( int(pred[i]==gold[i]) for i in set(gold) & set(pred) )
print('Correct mappings: %s' % n_correct)

# Calculate scores
precision = float(n_correct) / float(n_predicted)
print('Precision: %s' % precision )
recall = float(n_correct) / float(n_gold)
print('Recall: %s' % recall )
f1 = 2 * ( (precision * recall) / (precision + recall) )
print('F1: %s' % f1 )
