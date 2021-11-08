import pickle
from pathlib import Path

import pandas as pd

# config
indexName = "20211108-14_19_08_index.pickle"
postings_dir = "E:/postings_lists/"
tweets_dir = "E:/tweets/"
showResults = 20
index = None

# functions
def query(term):
    if term in index:
        # read csv file from disk and return values as list
        postings = pd.read_csv(postings_dir + 'p' + str(index[term]['postings']) + '.csv')
        return postings.iloc[:, 0].values.tolist()
    else:
        # term not in index, return empty list
        return []

def queryAND(term1, term2):
    # get postings lists
    list1 = iter(query(term1))
    list2 = iter(query(term2))

    # get start values smallValue, bigValue not sorted yet!
    smallV = next(list1, None)
    bigV = next(list2, None)

    # if one term has no results, AND query has no results
    if smallV == None or bigV == None:
        return []

    # init conditions list1 with smallV tries to catchup to list2 to bigV
    if (smallV > bigV):
        temp = smallV
        temp2 = list1
        smallV = bigV
        list1 = list2
        bigV = temp
        list2 = temp2

    matches = []
    # intersect lists
    while (smallV <= bigV):
        # get value
        smallV = next(list1, None)

        # one list reached the end, break and return
        if (smallV == None):
            break

        # match found!
        if (smallV == bigV):
            matches.append(smallV)

        # smallValue passed bigValue => swap values and lists
        if (smallV > bigV):
            temp = smallV
            temp2 = list1
            smallV = bigV
            list1 = list2
            bigV = temp
            list2 = temp2

    return matches


def displayResults(postingslists, nOfResults=showResults):
    results = []

    # if results > nOfResults show only nOfResults many results
    maxIndex = len(postingslists)
    if nOfResults != 0 and nOfResults < maxIndex:
        maxIndex = nOfResults

    # if no results
    if maxIndex == 0:
        print("No results...")
        return results

    # iterate over ressults, open tweet.txt file from disk and print tweet in a line
    for i in range(0, maxIndex):
        file = tweets_dir + str(postingslists[i]) + ".txt"
        with open(file, 'r', encoding='utf-8') as f:
            results.append(str(postingslists[i])+" :    "+f.read())
    for i in range(0, len(results)):
        print(results[i])

    return results

# run
# check if index file exists
results = Path(indexName);
if results.is_file():

    # unpickle index file
    print("file found, loading...")
    pickleFile = open(indexName, 'rb')
    index = pickle.load(pickleFile)

    # UI loop, let user enter queries
    while True:
        queryString = input("Enter your search query: ")

        # exit program with q
        if (queryString == "q"):
            print("Quit called. Goodbye, see you later alligator...")
            break

        # process query
        terms = queryString.lower().split()
        if (len(terms) == 1):
            # one term, use simple query
            results = query(terms[0])
        else:
            # two terms, use AND query
            results = queryAND(terms[0], terms[1])

        # display results
        resultDocuments = displayResults(results)
