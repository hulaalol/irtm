import pickle
from pathlib import Path

import pandas as pd

# config
indexName = "20211108-12_36_17_index.pickle"
postings_dir = "E:/postings_lists/"
tweets_dir = "E:/tweets/"
showResults = 0
index = None


# functions
def preprocessQuery(query):
    return query.lower().split()


def query(term):
    if term in index:
        postings = pd.read_csv(postings_dir + 'p' + str(index[term]['postings']) + '.csv')
        return postings.iloc[:, 0].values.tolist()
    else:
        return []


def queryAND(term1, term2):
    list1 = query(term1)
    list2 = query(term2)
    matches = []

    if (len(list1) < len(list2)):
        short = iter(list1)
        long = iter(list2)
    else:
        short = iter(list2)
        long = iter(list1)

    smallV = next(short, None)
    bigV = next(long, None)

    if smallV == None or bigV == None:
        return []

    if (smallV > bigV):
        temp = smallV
        temp2 = short
        smallV = bigV
        short = long
        bigV = temp
        long = temp2

    while (smallV <= bigV):
        smallV = next(short, None)
        if (smallV == None):
            break
        if (smallV == bigV):
            matches.append(smallV)
        if (smallV > bigV):
            temp = smallV
            temp2 = short
            smallV = bigV
            short = long
            bigV = temp
            long = temp2

    return matches


def displayResults(postingslists, nOfResults=showResults):
    results = []
    maxIndex = len(postingslists)
    if nOfResults != 0 and nOfResults < maxIndex:
        maxIndex = nOfResults

    if maxIndex == 0:
        print("No results...")
        return results

    for i in range(0, maxIndex):
        file = tweets_dir + str(postingslists[i]) + ".txt"
        with open(file, 'r', encoding='utf-8') as f:
            results.append(f.read())

    for i in range(0, len(results)):
        print(results[i])

    return results


results = Path(indexName);

if results.is_file():

    print("file found, loading...")
    pickleFile = open(indexName, 'rb')
    index = pickle.load(pickleFile)

    while True:
        queryString = input("Enter your search query: ")
        if (queryString == "q"):
            print("Quit called. Goodbye, see you later alligator...")
            break

        terms = queryString.lower().split()
        if (len(terms) == 1):
            results = query(terms[0])
        else:
            results = queryAND(terms[0], terms[1])

        resultDocuments = displayResults(results)
