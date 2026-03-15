import pandas as pd

df = pd.read_csv("clean.csv")
query_df = pd.read_csv("query.csv")

query_df.head()

out = []
for idx, row in query_df.iterrows():

    diffsx = df[["x", "y"]] - row
    squares = diffsx**2
    dists = squares.sum(axis=1) ** 0.5
    sorted_dists = dists.sort_values()

    out_dic = {}
    for i in range(3):
        ith_closest_idx = sorted_dists.index[i]
        out_dic[f"top{i+1}_title"] = df.loc[ith_closest_idx, "title"]
        out_dic[f"top{i+1}_id"] = df.loc[ith_closest_idx, "imdb_id"]

    out.append(out_dic)

pd.DataFrame(out).to_csv("out.csv", index=False)
