import pandas as pd
import numpy as np

df = pd.read_csv("input.csv")

cleaned_df = df.loc[:, ["imdb_id", "title", "x", "y"]]

points = cleaned_df[["x", "y"]].to_numpy()
np.save("points.npy", points)

meta = cleaned_df[["title", "imdb_id"]].to_numpy()
np.save("meta.npy", meta)

