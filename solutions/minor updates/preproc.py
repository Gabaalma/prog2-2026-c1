import pandas as pd

df = pd.read_csv("input.csv")
cleaned_df = df.loc[:, ["imdb_id", "title", "x", "y"]]




cleaned_df.to_csv("clean.csv")