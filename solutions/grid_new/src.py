import csv

# 1. Beolvassuk a query-t a beépített, szupergyors csv modullal
# Így még a Pandas sem kell ahhoz, hogy eldöntsük, melyik ágon indulunk el!
with open("query.csv", "r", encoding="utf-8") as f:
    queries = list(csv.DictReader(f))

num_queries = len(queries)

# --- DÖNTÉSI LOGIKA ---
if num_queries <= 50:
    # === GYORS ÁG (Kicsi adatokhoz) ===
    # ITT SEMMILYEN KÜLSŐ CSOMAGOT NEM TÖLTÜNK BE!
    
    with open("input.csv", "r", encoding="utf-8") as f:
        raw_movies = list(csv.DictReader(f))
    
    # Előkészítjük a filmeket (b^2 kiszámolása)
    movies = []
    for m in raw_movies:
        mx, my = float(m['x']), float(m['y'])
        movies.append({
            'title': m['title'], 'id': m['imdb_id'], 
            'x': mx, 'y': my, 'm_sq': mx*mx + my*my
        })
    
    geciskalacs = []
    for q in queries:
        qx, qy = float(q['x']), float(q['y'])
        q_sq = qx*qx + qy*qy
        
        dists = []
        for m in movies:
            d2 = q_sq + m['m_sq'] - 2 * (qx * m['x'] + qy * m['y'])
            dists.append((d2, m['title'], m['id']))
        
        dists.sort()
        res = [dists[0][1], dists[0][2], dists[1][1], dists[1][2], dists[2][1], dists[2][2]]
        geciskalacs.append(res)
        
    # A Pandas helyett a beépített csv írót használjuk az eredmény mentéséhez
    with open("out.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
        writer.writerows(geciskalacs)

else:
    # === GRID ÁG (Nagy adatokhoz) ===
    # A LASSÚ CSOMAGOKAT CSAK ITT TÖLTJÜK BE!
    # Ha a kód a fenti (gyors) ágon fut, a gép ezekkel a sorokkal nem is találkozik.
    import pandas as pd
    import numpy as np
    import json
    import pyarrow.parquet as pq

    # A már beolvasott 'queries' listából gyorsan csinálunk egy DataFrame-et
    query_df = pd.DataFrame(queries)
    query_df['x'] = query_df['x'].astype(float)
    query_df['y'] = query_df['y'].astype(float)
    
    # --- GRID MÓDSZER (Nagy adatokhoz) ---
    # Ez a te korábbi, Parquet-alapú kódod
    pf = pq.ParquetFile("grid_data.parquet", memory_map=True)
    df = pf.read().to_pandas()

    with open('grid_params.json', 'r') as f:
        params = json.load(f)
    n, x_min, x_max, y_min, y_max = params['n'], params['x_min'], params['x_max'], params['y_min'], params['y_max']
    x_step, y_step = (x_max - x_min) / n, (y_max - y_min) / n

    geciskalacs = []
    for row in query_df.itertuples():
        qx, qy = row.x, row.y
        i = max(0, min(n-1, int((qx - (x_min + x_step / 2)) / x_step)))
        j = max(0, min(n-1, int((qy - (y_min + y_step / 2)) / y_step)))
        
        grid_cell = df.iloc[i * n + j]
        # NumPy vektorizált távolság a cellán belül
        dx = np.array(grid_cell['x_coords']) - qx
        dy = np.array(grid_cell['y_coords']) - qy
        d2_array = dx*dx + dy*dy
        top_idx = np.argsort(d2_array)[:3]
        
        res = []
        for idx in top_idx:
            res.extend([grid_cell['titles'][idx], grid_cell['imdb_ids'][idx]])
        while len(res) < 6: res.append("")
        geciskalacs.append(res)

# --- KÖZÖS KIÍRÁS ---
out = pd.DataFrame(geciskalacs, columns=["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
out.to_csv("out.csv", index=False)