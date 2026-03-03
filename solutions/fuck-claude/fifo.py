import numpy as np
import pandas as pd
from sklearn.neighbors import KDTree

DTYPE = np.float32

data = pd.read_csv("input.csv")
tree = KDTree(data[["x", "y"]].astype(DTYPE), leaf_size=1)

titles = data["title"].values
ids = data["imdb_id"].values
rows = [f'"{title}",{id}' for title, id in zip(titles, ids)]

open("done.pipe", "w").write("ready")

open("trigger.pipe", "r").read()

query = np.loadtxt("query.csv", delimiter=",", dtype=DTYPE, skiprows=1)
_, inds = tree.query(query, k=3)

with open("out.csv", "w") as f:
    f.write("top1_title,top1_id,top2_title,top2_id,top3_title,top3_id\n")
    f.write("\n".join(f"{rows[t[0]]},{rows[t[1]]},{rows[t[2]]}" for t in inds))
