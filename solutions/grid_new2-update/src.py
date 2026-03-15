import csv

# 1. Beolvassuk a query-t a beépített, szupergyors csv modullal
with open("query.csv", "r", encoding="utf-8") as f:
    queries = list(csv.DictReader(f))

num_queries = len(queries)
geciskalacs = []

# --- DÖNTÉSI LOGIKA ---
if num_queries <= 50:
    # === GYORS ÁG (Tiszta Python, NULLA NumPy overhead) ===
    # Nem importálunk semmit, csak az alapvető Python loop-okat használjuk.
    
    with open("input.csv", "r", encoding="utf-8") as f:
        raw_movies = list(csv.DictReader(f))
    
    # Előkészítjük a filmeket (b^2 kiszámolása egyszer az elején)
    movies = []
    for m in raw_movies:
        mx, my = float(m['x']), float(m['y'])
        movies.append({
            'title': m['title'], 'id': m['imdb_id'], 
            'x': mx, 'y': my, 
            'm_sq': mx*mx + my*my  # b^2 tag
        })
    
    for q in queries:
        qx, qy = float(q['x']), float(q['y'])
        q_sq = qx*qx + qy*qy  # a^2 tag
        
        dists = []
        for m in movies:
            # d^2 = a^2 + b^2 - 2ab (távolság-algebra)
            d2 = q_sq + m['m_sq'] - 2 * (qx * m['x'] + qy * m['y'])
            dists.append((d2, m['title'], m['id']))
        
        # Sima Python rendezés - 1000 elemnél ez gyorsabb, mint a NumPy betöltése
        dists.sort()
        res = [dists[0][1], dists[0][2], dists[1][1], dists[1][2], dists[2][1], dists[2][2]]
        geciskalacs.append(res)
        
    # Mentés szintén Pandas nélkül
    with open("out.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
        writer.writerows(geciskalacs)

else:
    # === GRID ÁG (Nagy adatokhoz, vektorizált NumPy turbó) ===
    # A nehéz csomagokat CSAK ITT töltjük be, így a kicsiknél megspóroljuk az idejüket.
    import numpy as np
    import json
    import pickle

    # 1. Grid paraméterek betöltése
    with open('grid_params.json', 'r') as f:
        params = json.load(f)
    n, x_min, x_max, y_min, y_max = params['n'], params['x_min'], params['x_max'], params['y_min'], params['y_max']
    x_step, y_step = (x_max - x_min) / n, (y_max - y_min) / n

    # 2. Grid adatok betöltése (Pre-compiled NumPy tömbök)
    data = np.load('grid_data.npz', allow_pickle=True)
    imdb_grid = data['imdb']
    title_grid = data['title']
    x_grid = data['x']
    y_grid = data['y']

    # 3. Összes query koordináta kinyerése és vektorizálása
    q_xs = np.array([float(q['x']) for q in queries], dtype=np.float32)
    q_ys = np.array([float(q['y']) for q in queries], dtype=np.float32)

    # 4. VEKTORIZÁCIÓ: Minden lekérdezés cellaindexét egyszerre számoljuk ki
    i_indices = np.clip(((q_xs - x_min) / x_step).astype(int), 0, n - 1)
    j_indices = np.clip(((q_ys - y_min) / y_step).astype(int), 0, n - 1)
    grid_idxs = i_indices * n + j_indices

    # 5. Számítási ciklus a hálón belül
    for k in range(num_queries):
        qx, qy = q_xs[k], q_ys[k]
        idx = grid_idxs[k]
        
        cell_x, cell_y = x_grid[idx], y_grid[idx]
        
        # Ha üres a cella, üres sort adunk hozzá (sorfolytonosság megőrzése)
        if len(cell_x) == 0:
            geciskalacs.append([""] * 6)
            continue
            
        # Gyors NumPy távolságszámítás a cellán belül
        dx, dy = cell_x - qx, cell_y - qy
        d2 = dx*dx + dy*dy
        
        top_indices = np.argsort(d2)[:3]
        
        res = []
        for t_idx in top_indices:
            res.extend([title_grid[idx][t_idx], imdb_grid[idx][t_idx]])
        
        while len(res) < 6: res.append("")
        geciskalacs.append(res)

    # Mentés a beépített csv íróval az egységesség kedvéért
    with open("out.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id"])
        writer.writerows(geciskalacs)