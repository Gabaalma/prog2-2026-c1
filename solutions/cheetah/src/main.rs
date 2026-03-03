use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};

fn main() {
    match env::args().nth(1).as_deref() {
        Some("preproc") => preproc(),
        Some("compute") => compute(),
        _ => panic!("usage: cheetah <preproc|compute>"),
    }
}

fn preproc() {
    let mut rdr = csv::Reader::from_path("input.csv").unwrap();
    let headers = rdr.headers().unwrap().clone();

    let col = |name: &str| headers.iter().position(|h| h == name).unwrap();
    let (x_col, y_col, title_col, id_col) =
        (col("x"), col("y"), col("title"), col("imdb_id"));

    let mut xs: Vec<f32> = Vec::new();
    let mut ys: Vec<f32> = Vec::new();
    let mut titles: Vec<String> = Vec::new();
    let mut ids: Vec<String> = Vec::new();

    for result in rdr.records() {
        let rec = result.unwrap();
        xs.push(rec[x_col].parse().unwrap());
        ys.push(rec[y_col].parse().unwrap());
        titles.push(rec[title_col].to_string());
        ids.push(rec[id_col].to_string());
    }

    let n = xs.len() as u32;
    let mut out = BufWriter::new(File::create("data.bin").unwrap());

    out.write_all(&n.to_le_bytes()).unwrap();
    for &x in &xs {
        out.write_all(&x.to_le_bytes()).unwrap();
    }
    for &y in &ys {
        out.write_all(&y.to_le_bytes()).unwrap();
    }
    // titles then ids, each prefixed with u32 length
    for s in titles.iter().chain(ids.iter()) {
        let b = s.as_bytes();
        out.write_all(&(b.len() as u32).to_le_bytes()).unwrap();
        out.write_all(b).unwrap();
    }
}

fn compute() {
    let data = fs::read("data.bin").unwrap();
    let mut pos = 0;

    let n = read_u32(&data, &mut pos) as usize;

    let xs: Vec<f32> = (0..n).map(|_| read_f32(&data, &mut pos)).collect();
    let ys: Vec<f32> = (0..n).map(|_| read_f32(&data, &mut pos)).collect();

    let mut strings: Vec<String> = Vec::with_capacity(2 * n);
    for _ in 0..2 * n {
        let len = read_u32(&data, &mut pos) as usize;
        strings.push(String::from_utf8(data[pos..pos + len].to_vec()).unwrap());
        pos += len;
    }
    let titles = &strings[..n];
    let ids = &strings[n..];

    // Read queries
    let mut rdr = csv::Reader::from_path("query.csv").unwrap();
    let headers = rdr.headers().unwrap().clone();
    let x_col = headers.iter().position(|h| h == "x").unwrap();
    let y_col = headers.iter().position(|h| h == "y").unwrap();

    let mut queries: Vec<(f32, f32)> = Vec::new();
    for result in rdr.records() {
        let rec = result.unwrap();
        queries.push((rec[x_col].parse().unwrap(), rec[y_col].parse().unwrap()));
    }

    // Pre-allocate distance buffer (reused per query)
    let mut dists = vec![0.0f32; n];

    let out_file = File::create("out.csv").unwrap();
    let mut wtr = csv::Writer::from_writer(BufWriter::new(out_file));
    wtr.write_record(&[
        "top1_title", "top1_id", "top2_title", "top2_id", "top3_title", "top3_id",
    ])
    .unwrap();

    for (qx, qy) in &queries {
        // Compute all squared distances (auto-vectorized by compiler)
        for (i, (&x, &y)) in xs.iter().zip(ys.iter()).enumerate() {
            let dx = x - qx;
            let dy = y - qy;
            dists[i] = dx * dx + dy * dy;
        }

        // Find top 3 via linear scan with insertion into sorted triple
        let mut top3: [(f32, usize); 3] = [(f32::MAX, 0); 3];
        for (i, &d) in dists.iter().enumerate() {
            if d < top3[2].0 {
                top3[2] = (d, i);
                if top3[2].0 < top3[1].0 {
                    top3.swap(1, 2);
                    if top3[1].0 < top3[0].0 {
                        top3.swap(0, 1);
                    }
                }
            }
        }

        wtr.write_record(&[
            &titles[top3[0].1],
            &ids[top3[0].1],
            &titles[top3[1].1],
            &ids[top3[1].1],
            &titles[top3[2].1],
            &ids[top3[2].1],
        ])
        .unwrap();
    }
    wtr.flush().unwrap();
}

#[inline(always)]
fn read_u32(data: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    v
}

#[inline(always)]
fn read_f32(data: &[u8], pos: &mut usize) -> f32 {
    let v = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    v
}
