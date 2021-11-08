import csv
import sys

import numpy as np
import nltk
from nltk.corpus import stopwords
import pandas as pd
import pickle
from pathlib import Path


nrows = 20000
indexName = "final.pickle"
results = Path(indexName);

if results.is_file():
    print("file found, loading...")
    pickleFile = open(indexName, 'rb')
    results = pickle.load(pickleFile)

    #convert index
    fileId = 0

    for term in results:
        entry = results[term]
        fileName = str(fileId)+'.csv'
        file = open('E:/postings_lists/p'+fileName, 'w', newline='')
        writer = csv.writer(file)
        postings = sorted(list(entry['postings']))
        writer.writerows([[handle] for handle in postings])
        entry['postings'] = fileId
        fileId = fileId+1

    print(results)

else:
    print("file not found, rebuilding index...")
    #print(sys.maxsize)
    nltk.download('stopwords')
    stopwords = stopwords.words('english')
    print("reading csv...")
    if(nrows>0):
        data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str }, header=None, nrows=nrows)
    else:
        data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str}, header=None)


    def normalize(line):
        text = line.tweet
        if(type(text) != str):
            return ""

        for ch in ['[NEWLINE]', '[TAB]', '\\', '`', '*', '_', '{', '}', '[', ']', '(', ')', '>', '+', '-', '.', '!', '?', '$', '\'', '"', '/']:
            if ch in text:
                text = text.replace(ch, " ")
        return text.lower().split()

    #idea 1: add postings lists to map, then remove and save as file
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
            if(term in stopwords):
                continue

            if(term in results):
                entry = results[term]
                entry["docFreq"] = entry["docFreq"]+1
                entry["postings"].add(item.handle)
            else:
                results[term] = {"docFreq": 1, "postings": set([item.handle])}

    print("pickling results...")
    f = open(indexName, 'wb')
    pickle.dump(results, f)
    f.close()
    print("done.")