import itertools
import pickle
from pathlib import Path

import pandas as pd

# config
indexName = "20211124-11_34_06_index.pickle"
indexNameBigram = "20211124-15_06_00_kgram_index.pickle"
postings_dir = "E:/postings_lists/"
bigrams_dir = "E:/bigrams/"
tweets_dir = "E:/tweets/"
showResults = 10
index = None
bigramIndex = None

# functions
def queryBigram(bigram):
    if bigram in bigramIndex:
        # read csv file from disk and return values as list
        postings = pd.read_csv(bigrams_dir + 'b' + str(bigramIndex[bigram]['postings']) + '.csv', header=None)
        return postings.iloc[:, 0].values.tolist()
    else:
        # term not in index, return empty list
        print(bigram+" not in bigramIndex!")
        return []

def queryBigramAND(bigrams):
    if(len(bigrams) == 1):
        return queryBigram(bigrams[0])
    else:
        list1 = iter(queryBigram(bigrams[0]))
        list2 = iter(queryBigram(bigrams[1]))
        matches = intersectLists(list1, list2)

        for i in range(2, len(bigrams)):
            matches = intersectLists(iter(matches), iter(queryBigram(bigrams[i])))

    return matches


def query(term):
    if term in index:
        # read csv file from disk and return values as list
        postings = pd.read_csv(postings_dir + 'p' + str(index[term]['postings']) + '.csv', header=None)
        return postings.iloc[:, 0].values.tolist()

    else:
        # term not in index, return empty list
        return []

def queryAND(terms):
    if(len(terms) == 0):
        return []
    elif(len(terms) == 1):
        return query(terms[0])
    else:
        list1 = iter(query(terms[0]))
        list2 = iter(query(terms[1]))
        matches = intersectLists(list1, list2)

        for i in range(2, len(terms)):
            matches = intersectLists(iter(matches), iter(query(terms[i])))

    return matches

def intersectLists(list1, list2):
    # get start values smallValue, bigValue not sorted yet!
    smallV = next(list1, None)
    bigV = next(list2, None)

    matches = []
    if smallV == bigV:
        matches.append(smallV)

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

def getTerms(wildcardTerm):
    bigramQueries = []

    if(wildcardTerm[0] == "*" and wildcardTerm[-1] == "*"):
    # case: *mon*  --> mo on
        for i in range(1, len(wildcardTerm)-2):
            bigramQueries.append(wildcardTerm[i]+wildcardTerm[i+1])

        terms = queryBigramAND(bigramQueries)
        fragment = wildcardTerm[1:-1]
        temp = []
        for term in terms:
            if fragment in term:
                temp.append(term)
        terms = temp

    elif(wildcardTerm[-1] == "*"):
    # case: mon*   --> $m mo on
        for i in range(0, (len(wildcardTerm)-1)):
            if i == 0:
                bigramQueries.append("$"+wildcardTerm[i])
            else:
                bigramQueries.append(wildcardTerm[i-1]+wildcardTerm[i])

        terms = queryBigramAND(bigramQueries)

        prefix = wildcardTerm[:-1]
        temp = []
        for term in terms:
            termprefix = term[0:len(prefix)]
            if termprefix == prefix:
                temp.append(term)
        terms = temp

    elif(wildcardTerm[0] == "*"):
    # case: *mon   --> mo on n$
        for i in range(1, len(wildcardTerm)):
            if i == (len(wildcardTerm)-1):
                bigramQueries.append(wildcardTerm[i]+"$")
            else:
                bigramQueries.append(wildcardTerm[i]+wildcardTerm[i+1])

        terms = queryBigramAND(bigramQueries)

        suffix = wildcardTerm[1:]
        temp = []
        for term in terms:
            termsuffix = term[-len(suffix):]
            if termsuffix == suffix:
                temp.append(term)
        terms = temp

    else:
        # case: mo*n   --> $m mo n$
        splits = wildcardTerm.split("*")
        bigramQueries.append("$" + splits[0][0])
        for i in range(0,(len(splits[0])-1)):
            bigramQueries.append(splits[0][i]+splits[0][i+1])
        for i in range(0, (len(splits[1])-1)):
            bigramQueries.append(splits[1][i]+splits[1][i+1])
        bigramQueries.append(splits[1][-1]+"$")
        terms = queryBigramAND(bigramQueries)


    print("bigrams queried: "+str(bigramQueries))
    print("terms matched: "+str(terms))
    return terms


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
results = Path(indexName)
if results.is_file():

    # unpickle index file
    print("index file found, loading...")
    pickleFile = open(indexName, 'rb')
    index = pickle.load(pickleFile)

    #load bigram index
    bigram = Path(indexNameBigram)
    if bigram.is_file():
        print("bigram index file found, loading...")
        pickleFile = open(indexNameBigram, 'rb')
        bigramIndex = pickle.load(pickleFile)
    else:
        print("bigram index file not found!")


    # UI loop, let user enter queries
    while True:
        queryString = input("Enter your search query: ")

        # exit program with q
        if (queryString == "q"):
            print("Quit called. Goodbye, see you later alligator...")
            break

        if (len(queryString) < 2):
            print("query string is too short! Type more :)")
            continue

        # process query
        terms = queryString.lower().split()

        # get all terms
        lists = []
        for term in terms:
            if '*' in term:
                lists.append(getTerms(term))
            else:
                lists.append([term])

        results = []
        if(len(lists) > 1):
            #get all combinations
            all_combinations = list(itertools.product(lists[0],lists[1]))
            #all_combinations = [list(zip(each_permutation, lists[1])) for each_permutation in itertools.permutations(lists[0], len(lists[1]))]
            print("There are "+str(len(all_combinations))+" combinations of query terms...")
            #AND query each combination and gather results
            for combo in all_combinations:
                q = list(combo)
                print("querying terms:"+str(q))
                results = results + queryAND(q)
        else:
            for term in lists[0]:
                results = results + query(term)

        # display results
        resultDocuments = displayResults(results)
