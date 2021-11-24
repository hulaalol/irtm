import csv
from datetime import datetime
from itertools import chain
import numpy as np
import nltk
import pandas as pd
import pickle

# This script builds a non-positional inverted index. Postings lists are stored as .csv files on disk.
# The index is stored as pickled python object on disk and can be loaded by the queryEngine later.

# The index is a dictionary with the term as key and the values docFrequency and postings
# docFrequency is an integer which counts how often the term occurs in all documents
# postings is an integer with the file ID for the postings list.
# E.g. postings=100 means, the posting list is stored in the file p100.csv


# config
indexName = datetime.today().strftime('%Y%m%d-%H_%M_%S') + "_index.pickle"
nrows = 10000


# token normalization
def normalize(line):
    # this function removes special characters, newlines and tab from tweets, tokenizes the text and sets all terms
    # to lowercase
    text = line.tweet
    if (type(text) != str):
        return ""

    for ch in ['[NEWLINE]', '[TAB]', '#', ',', ';', ':', '\\', '`', '*', '_', '{', '}', '[', ']',
               '(', ')', '>', '+', '-', '.', '!', '?', '$', '\'', '"', '/']:
        if ch in text:
            text = text.replace(ch, " ")
    return text.lower().split()


# index function
def index(filename):
    # read csv
    print("reading csv...")
    if (nrows > 0):
        data = pd.read_csv(filename, sep="\t",
                           names=["handle", "userid", "username", "tweet"],
                           dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str},
                           header=None,
                           nrows=nrows,
                           quoting=3)
    else:
        data = pd.read_csv(filename, sep="\t",
                           names=["handle", "userid", "username", "tweet"],
                           dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str},
                           header=None,
                           quoting=3)

    # get stopwords
    nltk.download('stopwords')
    languages = ['english', 'german', 'spanish', 'portuguese', 'italian', 'french', 'turkish', 'dutch']
    stopwords = dict.fromkeys([i for i in chain.from_iterable([nltk.corpus.stopwords.words(l) for l in languages])])

    # gather data
    progress = 0;
    print("building inverted index ...")
    results = {}
    for item in data.iloc:

        progress = progress + 1;
        if (progress % 10000 == 0):
            print("processing line " + str(progress) + " ...")

        # tokenize tweet
        terms = normalize(item)

        for term in terms:

            # ignore stopwords and single characters
            if term in stopwords or len(term) < 2:
                continue

            if term in results:
                entry = results[term]
                entry["docFreq"] = entry["docFreq"] + 1
                entry["postings"].add(item.handle)
            else:
                results[term] = {"docFreq": 1, "postings": set([item.handle])}

    # save postings lists as file
    print("create postings lists files...")
    fileId = 0

    for term in results:
        # for each term write postings lists to csv file on disk
        entry = results[term]
        fileName = str(fileId) + '.csv'
        file = open('E:/postings_lists/p' + fileName, 'w', newline='')
        writer = csv.writer(file)
        postings = sorted(list(entry['postings']))
        writer.writerows([[handle] for handle in postings])
        entry['postings'] = fileId
        fileId = fileId + 1

        if fileId % 10000 == 0:
            print("writing file " + str(fileId))

    # pickle index as file on disk
    print("pickling results...")
    f = open(indexName, 'wb')
    pickle.dump(results, f)
    f.close()
    print("done.")
    return results


# run
indexData = index("twitter.csv")
