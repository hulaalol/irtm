import csv

import pandas as pd
import numpy as np

nrows = 0
folder = "E:/tweets/"

# read csv
print("reading csv...")
if(nrows>0):
    data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str}, header=None, nrows=nrows, quoting=3)
else:
    data = pd.read_csv('twitter.csv', sep="\t", names=["handle", "userid", "username", "tweet"], dtype={"handle": np.int64, "userid": str, "username": str, "tweet": str}, header=None, quoting=3)

for item in data.iloc:
    with open(folder+str(item.handle)+'.txt', 'w', encoding='utf-8') as f:
        f.write(item.tweet)