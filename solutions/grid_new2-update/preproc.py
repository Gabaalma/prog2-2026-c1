import pandas as pd
import numpy as np
from collections import defaultdict
import json

# 1. Alapadatok beolvasása
print("Adatok beolvasása és globális matek...")
df = pd.read_csv("input.csv")

# Kiszámoljuk a b^2 (m_sq) értéket minden filmre a legelején
# b^2 = x^2 + y^2
df['m_sq'] = (df['x']**2 + df['y']**2).astype(np.float32)

cleaned_df = df.loc[:, ["imdb_id", "title", "x", "y", "m_sq"]]
coords = cleaned_df[['x', 'y']].values

# --- MENTÉS 1: GLOBÁLIS ADATOK (A gyors ágnak) ---
# Ez a fájl kicsi lesz, és csak az alapokat tartalmazza
np.savez_compressed('global_movies.npz', 
                    imdb=cleaned_df['imdb_id'].values,
                    title=cleaned_df['title'].values,
                    x=cleaned_df['x'].values.astype(np.float32),
                    y=cleaned_df['y'].values.astype(np.float32),
                    m_sq=cleaned_df['m_sq'].values)

# 2. Háló (Grid) generálása
n = 50 
x_min, x_max = -11.5, 12
y_min, y_max = -11, 9.5
x_step = (x_max - x_min) / n
y_step = (y_max - y_min) / n

x_centers = [x_min + x_step / 2 + i * x_step for i in range(n)]
y_centers = [y_min + y_step / 2 + i * y_step for i in range(n)]

# 3. Pontok szétosztása cellákba
cell_data = defaultdict(lambda: {'ids': [], 'titles': [], 'x': [], 'y': []})
for _, row in cleaned_df.iterrows():
    i = max(0, min(n-1, int((row['x'] - x_min) / x_step)))
    j = max(0, min(n-1, int((row['y'] - y_min) / y_step)))
    cell_data[(i, j)]['ids'].append(row['imdb_id'])
    cell_data[(i, j)]['titles'].append(row['title'])
    cell_data[(i, j)]['x'].append(row['x'])
    cell_data[(i, j)]['y'].append(row['y'])

# 4. "Szuper-cellák" összeállítása (Szomszédok + Biztonsági Top 3)
output_points = []
for i in range(n):
    for j in range(n):
        x_c, y_c = x_centers[i], y_centers[j]
        id_to_data = {}
        
        # Jelenlegi cella + szomszédok
        for di in [-1, 0, 1]:
            for dj in [-1, 0, 1]:
                ni, nj = i + di, j + dj
                if 0 <= ni < n and 0 <= nj < n:
                    c = cell_data[(ni, nj)]
                    for k in range(len(c['ids'])):
                        id_to_data[c['ids'][k]] = (c['titles'][k], c['x'][k], c['y'][k])
        
        # Globális biztonsági Top 3 (ha a környék üres lenne)
        dists = (coords[:, 0] - x_c) ** 2 + (coords[:, 1] - y_c) ** 2
        nearest_idxs = np.argpartition(dists, 3)[:3]
        for idx in nearest_idxs:
            row = cleaned_df.iloc[idx]
            if row['imdb_id'] not in id_to_data:
                id_to_data[row['imdb_id']] = (row['title'], row['x'], row['y'])

        output_points.append({
            'ids': list(id_to_data.keys()),
            'titles': [v[0] for v in id_to_data.values()],
            'x': [v[1] for v in id_to_data.values()],
            'y': [v[2] for v in id_to_data.values()]
        })

# --- MENTÉS 2: GRID ADATOK (A nagy ágnak) ---
print("Grid adatok mentése...")
imdb_grid = np.array([p['ids'] for p in output_points], dtype=object)
title_grid = np.array([p['titles'] for p in output_points], dtype=object)
x_grid = np.array([np.array(p['x'], dtype=np.float32) for p in output_points], dtype=object)
y_grid = np.array([np.array(p['y'], dtype=np.float32) for p in output_points], dtype=object)

np.savez_compressed('grid_data.npz', imdb=imdb_grid, title=title_grid, x=x_grid, y=y_grid)

# 5. Paraméterek mentése JSON-be
with open("grid_params.json", "w") as f:
    json.dump({"n": n, "x_min": x_min, "x_max": x_max, "y_min": y_min, "y_max": y_max}, f)

print("Pre-processing kész: global_movies.npz, grid_data.npz és grid_params.json létrehozva.")