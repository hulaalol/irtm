import pandas as pd
import numpy as np

nrows = 0
folder = "E:/tweets/"

# read csv
print("reading csv...")
if(nrows>0):
    # only read nrows number of tweets
    data = pd.read_csv('twitter.csv',   sep="\t",
                                        names=["handle", "userid", "username", "tweet"],
                                        dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str},
                                        header=None, nrows=nrows,
                                        quoting=3)
else:
    # read all tweets
    data = pd.read_csv('twitter.csv',   sep="\t",
                                        names=["handle", "userid", "username", "tweet"],
                                        dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str},
                                        header=None,
                                        quoting=3)

# export tweets to txt files where filname is equal to the tweet handle
for item in data.iloc:
    with open(folder+str(item.handle)+'.txt', 'w', encoding='utf-8') as f:
        f.write(item.tweet)