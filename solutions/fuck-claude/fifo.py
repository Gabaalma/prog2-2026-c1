import numpy as np
import pandas as pd
from sklearn.neighbors import KDTree

DTYPE = np.float32

o_fp = open("out.csv", "wb")
o_fp.write(b"top1_title,top1_id,top2_title,top2_id,top3_title,top3_id\n")


def load_query_fast():
    with open("query.csv", "rb") as f:
        next(f)  # skip header
        data = []
        for line in f:
            x, y = line.rstrip().split(b",")
            data.append((float(x), float(y)))
    return np.array(data, dtype=DTYPE)


data = pd.read_csv("input.csv")
tree = KDTree(data[["x", "y"]].astype(DTYPE), leaf_size=1)

titles = data["title"].values
ids = data["imdb_id"].values
rows = [f'"{title}",{id}'.encode() for title, id in zip(titles, ids)]

open("done.pipe", "w").write("ready")

open("trigger.pipe", "r").read()

query = load_query_fast()
_, inds = tree.query(query, k=3)

output = bytearray()
for tops in inds:
    output.extend(rows[tops[0]])
    output.extend(b",")
    output.extend(rows[tops[1]])
    output.extend(b",")
    output.extend(rows[tops[2]])
    output.extend(b"\n")

o_fp.write(output)
o_fp.close()


with open("done.pipe", "w") as d:
    d.write("0\n")
