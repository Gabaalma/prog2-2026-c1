import pandas as pd
import numpy as np

df = pd.read_csv("input.csv")
cleaned_df = df.loc[:, ["imdb_id", "title", "x", "y"]]
cleaned_df.to_csv("clean.csv")

import math as math

def distance (p1, p2):
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) **2)

def stripClosest (strip, d): #ez a felező vonal körüli d sávban találja meg a legkisebb távolságot két pont között
    min_dist = d

    strip.sort(key = lambda point: point[1]) #y szerint rak sorba
    for i in range (len (strip)):
        for j in range (i +1, len(strip)): 
                    if (strip[j][1] - strip[i][1]) < min_dist: #itt addig csekkolom csak az i ponthoz közeli pontokat amíg az y koordináták közötti távolság nem lesz nagyobb mint a márkotábban megtalált d távolság
                          min_dist = min(min_dist, distance (strip[i], strip[j]) )
                    else:
                          break
    return min_dist

def minDistUtil(points, left, right): # ez visszaadja a legkisebb távolságot az összes pont között, oszd meg és uralkodj

    if right - left <= 2:
        min_dist = float('inf') #ha túl kevés pont lenne inf kapok vissza
        for i in range(left, right):
            for j in range(i + 1, right):
                min_dist = min(min_dist, distance(points[i], points[j]))
        return min_dist
     
    mid = (left + right) // 2
    mid_x = points[mid][0]
    dl = minDistUtil(points, left, mid) #rekuzív meghívás, addig osztogatja kétfelé amíg brute force
    dr = minDistUtil(points, mid, right)

    d = min(dl, dr)

    strip = []
    for i in range(left, right):
        if abs(points[i][0] - mid_x) < d:
            strip.append(points[i])

    stripDist = stripClosest(strip, d)
    return min(d, stripDist)

def minDistance(points):
    points = sorted(points, key =lambda p: p[0])
    return minDistUtil (points, 0, len(points))
    
points = cleaned_df[["x", "y"]].to_numpy().tolist()

dmin = minDistance(points)

np.save("dmin.npy", dmin)

X = cleaned_df["x"].to_numpy()
Y = cleaned_df["y"].to_numpy()

def dmax_3_by_expanding_box(X, Y, dmin, grow= 1.5, max_iters=50):

    n = len(X)
    d3 = np.empty(n, dtype=float)

    for i in range(n):
        qx, qy = X[i], Y[i]
        half = float(dmin) + 1e-12
        for _ in range(max_iters):
            mask = (np.abs(X - qx) <= half) & (np.abs(Y - qy) <= half)
            idxs = np.flatnonzero(mask)

            if idxs.size >= 3:
                dx = X[idxs] - qx
                dy = Y[idxs] - qy
                d2 = dx**2 + dy**2
                
                third_d2 = np.partition(d2, 2)[2]
                d3[i] = math.sqrt(float(third_d2))

                
            half *= grow
    else:
        dx = X - qx 
        dy = Y - qy
        d2 = dx*dx + dy*dy
        d2[i] = np.inf
        third_d2 = np.partition(d2, 2)[2]
        d3[i] = math.sqrt(float(third_d2))

    return d3
    
d3 = dmax_3_by_expanding_box(X, Y, dmin, grow=1.5, max_iters=50)
p99 = np.percentile(d3, 99)

np.save("p99.npy", p99)

