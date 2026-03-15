import pandas as pd
import numpy as np
import json
import heapq
import pyarrow.parquet as pq

query_df = pd.read_csv("query.csv")

# load parquet using pyarrow with memory mapping for speed
pf = pq.ParquetFile("grid_data.parquet", memory_map=True)
df = pf.read().to_pandas()

# Load grid parameters from preproc
with open('grid_params.json', 'r') as f:
    params = json.load(f)
n = params['n']
x_min = params['x_min']
x_max = params['x_max']
y_min = params['y_min']
y_max = params['y_max']
x_step = (x_max - x_min) / n
y_step = (y_max - y_min) / n





geciskalacs = []
for row in query_df.itertuples(index=False):
    qx, qy = row.x, row.y  # Access columns directly
    
    # Determine which grid cell the query belongs to
    i = int((qx - (x_min + x_step / 2)) / x_step)
    i = max(0, min(n-1, i))
    j = int((qy - (y_min + y_step / 2)) / y_step)
    j = max(0, min(n-1, j))
    
    # Compute a flat index for the cell
    grid_row_idx = i * n + j
    grid_cell = df.iloc[grid_row_idx]
    imdb_ids = grid_cell['imdb_ids']
    titles = grid_cell['titles']
    x_coords = grid_cell['x_coords']
    y_coords = grid_cell['y_coords']
    
    # Compute distances to these points only using NumPy vectorization
    if len(imdb_ids) == 0:
        continue
    
    # Vectorize distance calculation with NumPy
    x_coords_array = np.array(x_coords)
    y_coords_array = np.array(y_coords)
    dx = x_coords_array - qx
    dy = y_coords_array - qy
    dists = dx * dx + dy * dy  # Faster than ** 2
    top_indices = np.argsort(dists)[:3]  # Get indices of 3 smallest distances
    
    # Build result row with top 3
    result = []
    for idx_inner in top_indices:
        result.append(titles[idx_inner])
        result.append(imdb_ids[idx_inner])
    
    # Pad with empty strings if less than 3 points
    while len(result) < 6:
        result.append("")
    
    geciskalacs.append(result)

out = pd.DataFrame(geciskalacs, columns=["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
out.to_csv("out.csv", index=False)