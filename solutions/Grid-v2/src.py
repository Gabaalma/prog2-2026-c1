import pandas as pd
import numpy as np
import json
import pickle

# 1. Load queries and convert immediately to fast NumPy arrays
query_df = pd.read_csv("query.csv")
q_x = query_df['x'].values
q_y = query_df['y'].values
num_queries = len(q_x)

# 2. Load grid parameters
with open('grid_params.json', 'r') as f:
    params = json.load(f)
n = params['n']
x_min = params['x_min']
x_max = params['x_max']
y_min = params['y_min']
y_max = params['y_max']
x_step = (x_max - x_min) / n
y_step = (y_max - y_min) / n

# 3. Load the pre-compiled C-level NumPy arrays (allow_pickle=True is needed for object arrays)
# This completely replaces the slow Parquet file
# 3. Load the pickled data (which now contains ready-to-use NumPy arrays)

with open('grid_arrays.pkl', 'rb') as f:
    arrays = pickle.load(f)

imdb_arr = arrays['imdb_ids']
title_arr = arrays['titles']
x_arr = arrays['x_coords']
y_arr = arrays['y_coords']


# 4. VECTORIZATION: Calculate grid indices for ALL queries at once instantly
i_indices = ((q_x - (x_min + x_step / 2)) / x_step).astype(int)
i_indices = np.clip(i_indices, 0, n - 1)  # Replaces max(0, min(n-1, i))

j_indices = ((q_y - (y_min + y_step / 2)) / y_step).astype(int)
j_indices = np.clip(j_indices, 0, n - 1)

# Compute flat indices for all queries simultaneously
grid_idxs = i_indices * n + j_indices

# 5. The Execution Loop
geciskalacs = []

for k in range(num_queries):
    qx = q_x[k]
    qy = q_y[k]
    idx = grid_idxs[k]
    
    # Instantly pull the pre-compiled numpy arrays for this specific cell
    # No more list-to-array conversions here!
    cell_x = x_arr[idx]
    cell_y = y_arr[idx]
    cell_titles = title_arr[idx]
    cell_imdbs = imdb_arr[idx]
    
    if len(cell_imdbs) == 0:
        continue
    
    # Fast vectorized distance math
    dx = cell_x - qx
    dy = cell_y - qy
    dists = dx * dx + dy * dy
    
    # Get indices of the 3 closest movies
    top_indices = np.argsort(dists)[:3]
    
    # Build result row
    result = []
    for inner_idx in top_indices:
        result.append(cell_titles[inner_idx])
        result.append(cell_imdbs[inner_idx])
    
    # Pad with empty strings if less than 3 points
    while len(result) < 6:
        result.append("")
    
    geciskalacs.append(result)

# 6. Export results
out = pd.DataFrame(geciskalacs, columns=["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
out.to_csv("out.csv", index=False)