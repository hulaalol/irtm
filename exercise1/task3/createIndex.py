import csv
from datetime import datetime

import numpy as np
import nltk
from nltk.corpus import stopwords
import pandas as pd
import pickle

# helper function
def normalize(line):
    text = line.tweet
    if(type(text) != str):
        return ""

    for ch in ['[NEWLINE]', '[TAB]', '#', '\\', '`', '*', '_', '{', '}', '[', ']', '(', ')', '>', '+', '-', '.', '!', '?', '$', '\'', '"', '/']:
        if ch in text:
            text = text.replace(ch, " ")
    return text.lower().split()

# config
indexName = datetime.today().strftime('%Y%m%d-%H_%M_%S')+"_index.pickle"
nrows = 20000
# get stopwords
nltk.download('stopwords')
english = stopwords.words('english')
german = stopwords.words('german')
spanish = stopwords.words('spanish')
portuguese = stopwords.words('portuguese')
italian = stopwords.words('italian')
french = stopwords.words('french')
turkish = stopwords.words('turkish')
stopwords = dict.fromkeys([*english, *german, *spanish, *portuguese, *italian, *french, *turkish])

# read csv
print("reading csv...")
if(nrows>0):
    data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str }, header=None, nrows=nrows, quoting=3)
else:
    data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str}, header=None, quoting=3)

# gather data
progress = 0;
print("building inverted index ...")
results = {}
for item in data.iloc:

    progress = progress+1;
    if(progress % 10000 == 0):
        print("processing line "+str(progress)+" ...")

    terms = normalize(item)

    for term in terms:

        # ignore stopwords
        if term in stopwords or len(term) < 2:
            continue

        if term in results:
            entry = results[term]
            entry["docFreq"] = entry["docFreq"]+1
            entry["postings"].add(item.handle)
        else:
            results[term] = {"docFreq": 1, "postings": set([item.handle])}


# save postings lists as file
print("create postings lists files...")
fileId = 0

for term in results:
    entry = results[term]
    fileName = str(fileId) + '.csv'
    file = open('E:/postings_lists/p' + fileName, 'w', newline='')
    writer = csv.writer(file)
    postings = sorted(list(entry['postings']))
    writer.writerows([[handle] for handle in postings])
    entry['postings'] = fileId
    fileId = fileId + 1

    if fileId % 10000 == 0:
        print("writing file "+str(fileId))

print("pickling results...")
f = open(indexName, 'wb')
pickle.dump(results, f)
f.close()
print("done.")