import pandas as pd
import numpy as np
from collections import defaultdict
import json
import pyarrow.parquet as pq

df = pd.read_csv("input.csv")
cleaned_df = df.loc[:, ["imdb_id", "title", "x", "y"]]

# Generate  grid 
n = 50 #nxn meretu grid

x_min, x_max = -13, 13
y_min, y_max = -13, 13
x_step = (x_max - x_min) / n
y_step = (y_max - y_min) / n
x_centers = [x_min + x_step / 2 + i * x_step for i in range(n)]
y_centers = [y_min + y_step / 2 + i * y_step for i in range(n)]
points = []
for x in x_centers:
    for y in y_centers:
        points.append({'x': x, 'y': y})
grid_df = pd.DataFrame(points)

# Assign each cleaned_df point to the closest grid cell
cell_data = defaultdict(lambda: {'imdb_ids': [], 'titles': [], 'x_coords': [], 'y_coords': []})

for _, row in cleaned_df.iterrows():
    px, py = row['x'], row['y']
    i = int((px - (x_min + x_step / 2)) / x_step)
    i = max(0, min(n-1, i))
    j = int((py - (y_min + y_step / 2)) / y_step)
    j = max(0, min(n-1, j))
    
    cell_data[(i, j)]['imdb_ids'].append(row['imdb_id'])
    cell_data[(i, j)]['titles'].append(row['title'])
    cell_data[(i, j)]['x_coords'].append(row['x'])
    cell_data[(i, j)]['y_coords'].append(row['y'])


# After assigning each observation to its cell, compute 3 nearest neighbors per center
coords = cleaned_df[['x', 'y']].values

# Create output DataFrame with grid centers and associated data + nearest neighbors + neighbors' data
output_points = []
for i in range(n):
    for j in range(n):
        x = x_centers[i]
        y = y_centers[j]
        # start with existing cell data, using dicts to preserve id->coords mapping
        id_to_data = {}
        for idx, (imdb_id, title) in enumerate(zip(cell_data[(i, j)]['imdb_ids'], cell_data[(i, j)]['titles'])):
            id_to_data[imdb_id] = {
                'title': title,
                'x': cell_data[(i, j)]['x_coords'][idx],
                'y': cell_data[(i, j)]['y_coords'][idx]
            }

        # Add data from 8 neighboring cells (including diagonals)
        for di in [-1, 0, 1]:
            for dj in [-1, 0, 1]:
                if di == 0 and dj == 0:  # skip the current cell (already added)
                    continue
                ni, nj = i + di, j + dj
                if 0 <= ni < n and 0 <= nj < n:  # check bounds
                    for idx, (imdb_id, title) in enumerate(zip(cell_data[(ni, nj)]['imdb_ids'], cell_data[(ni, nj)]['titles'])):
                        if imdb_id not in id_to_data:
                            id_to_data[imdb_id] = {
                                'title': title,
                                'x': cell_data[(ni, nj)]['x_coords'][idx],
                                'y': cell_data[(ni, nj)]['y_coords'][idx]
                            }

        # compute distances to all observations
        dists = (coords[:, 0] - x) ** 2 + (coords[:, 1] - y) ** 2
        if len(dists) <= 3:
            nearest_idxs = np.arange(len(dists))
        else:
            nearest_idxs = np.argpartition(dists, 3)[:3]
        for idx in np.atleast_1d(nearest_idxs):
            imdb_id = cleaned_df.iloc[idx]['imdb_id']
            if imdb_id not in id_to_data:
                id_to_data[imdb_id] = {
                    'title': cleaned_df.iloc[idx]['title'],
                    'x': cleaned_df.iloc[idx]['x'],
                    'y': cleaned_df.iloc[idx]['y']
                }

        output_points.append({
            'x': x,
            'y': y,
            'imdb_ids': list(id_to_data.keys()),
            'titles': [id_to_data[iid]['title'] for iid in id_to_data.keys()],
            'x_coords': [id_to_data[iid]['x'] for iid in id_to_data.keys()],
            'y_coords': [id_to_data[iid]['y'] for iid in id_to_data.keys()]
        })

grid_with_data_df = pd.DataFrame(output_points)





grid_with_data_df.to_json("grid_data.json", orient="records", indent=2)

# also write binary Parquet for fast reading/memory-mapping
try:
    grid_with_data_df.to_parquet("grid_data.parquet", index=False)
except Exception:
    pass  # if engine not available, ignore

# for faster startup during compute, pickle the raw column lists so that
# src.py can bypass pandas/parquet entirely; pickle.load is very quick for
# a few megabytes of moderately structured data.
import pickle
arrays = {
    'imdb_ids': grid_with_data_df['imdb_ids'].tolist(),
    'titles': grid_with_data_df['titles'].tolist(),
    'x_coords': grid_with_data_df['x_coords'].tolist(),
    'y_coords': grid_with_data_df['y_coords'].tolist(),
}
with open('grid_arrays.pkl', 'wb') as f:
    pickle.dump(arrays, f, protocol=pickle.HIGHEST_PROTOCOL)

# Export grid parameters 

grid_params = {
    "n": n,
    "x_min": x_min,
    "x_max": x_max,
    "y_min": y_min,
    "y_max": y_max,
}
with open("grid_params.json", "w") as f:
    json.dump(grid_params, f)