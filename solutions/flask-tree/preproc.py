import flask
import pandas as pd
from sklearn.neighbors import KDTree

app = flask.Flask(__name__)

df = pd.read_csv("input.csv")
ids = df["imdb_id"].values
titles = df["title"].values
tree = KDTree(df[["x", "y"]])
cols = sum([[f"top{i + 1}_title", f"top{i + 1}_id"] for i in range(3)], [])


@app.route("/ping")
def ping():
    qdf = pd.read_csv("query.csv")
    _, inds = tree.query(qdf, k=3)
    out = pd.DataFrame(
        {
            f"top{i + 1}_{col}": arr[inds[:, i]]
            for i in range(3)
            for col, arr in [("title", titles), ("id", ids)]
        }
    )
    out.to_csv("out.csv", index=False)
    return "OK"


@app.route("/")
def ok():
    return "OK"


if __name__ == "__main__":
    app.run(port=5678)
