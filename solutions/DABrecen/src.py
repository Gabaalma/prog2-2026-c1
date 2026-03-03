import csv
import numpy as np
from scipy.spatial import cKDTree


points = np.load("points.npy")
meta = np.load("meta.npy", allow_pickle=True)

q_array = np.loadtxt("query.csv", delimiter=",", skiprows=1)

tree = cKDTree(
    points, 
    leafsize=10,           
    compact_nodes=False,   
    balanced_tree=False    
)

_, idxs = tree.query(q_array, k=3, workers=-1)

neighbors = meta[idxs]  # shape (n_queries, 3, 2)
out = neighbors.reshape(len(q_array), 6)

header = [
    "top1_title", "top1_id",
    "top2_title", "top2_id",
    "top3_title", "top3_id",
]

with open("out.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(out)