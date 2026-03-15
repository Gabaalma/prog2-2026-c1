import pandas as pd
import numpy as np
df = pd.read_csv("clean.csv")
query_df = pd.read_csv("query.csv")

dmin = np.load("dmin.npy")
p99 = np.load("p99.npy")

X = df["x"].to_numpy()
Y = df["y"].to_numpy()

def top3_by_expanding_box (X, Y, qx, qy, dmin, grow= 2, iters = 20):

    half = dmin + 1e-12
    
    for _ in range(iters):
        mask = (np.abs(X - qx) <= half) & (np.abs(Y - qy) <= half)
        idxs = np.flatnonzero(mask)

        if idxs.size >= 3:
            dx = X[idxs] - qx
            dy = Y[idxs] - qy
            d2 = dx**2 + dy**2
            best3_local = np.argpartition(d2, 2)[:3] # a 3 legközelebb lévő lokálisan
            best3 = idxs[best3_local]
            best3 = best3[np.argsort(d2[best3_local])]
            return best3

        half *= grow

    half = float(p99 + 1e-12)
    mask = (np.abs(X - qx) <= half) & (np.abs(Y - qy) <= half)
    idxs = np.flatnonzero(mask)

    if idxs.size >= 3:
        dx = X[idxs] - qx
        dy = Y[idxs] - qy
        d2 = dx*dx + dy*dy

        pick = np.argpartition(d2, 2)[:3]
        best3 = idxs[pick]
        order = np.argsort(d2[pick])
        return best3[order]

    dx = X - qx
    dy = Y - qy
    d2 = dx*dx + dy*dy

    best3 = np.argpartition(d2, 2)[:3]
    return best3[np.argsort(d2[best3])]

out = []
for _, q in query_df.iterrows():
    idxs = top3_by_expanding_box(X, Y, q["x"], q["y"], dmin)

    out_dic = {}
    for i, idx in enumerate(idxs, start=1):
        out_dic[f"top{i}_title"] = df.loc[idx, "title"]
        out_dic[f"top{i}_id"] = df.loc[idx, "imdb_id"]
    out.append(out_dic)

pd.DataFrame(out).to_csv("out.csv", index=False)
