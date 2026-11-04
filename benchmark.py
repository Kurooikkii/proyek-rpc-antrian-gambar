"""Uji kinerja antrian gambar gRPC (jobdesk C): latensi dan throughput untuk 1, 5, dan 20 client,
dibandingkan antara beberapa jumlah worker server (default 1 lawan 4).

Definisi pengukuran
  Latensi    : waktu dari awal upload sampai status pekerjaan DONE, per gambar (milidetik).
  Throughput : jumlah gambar berhasil dibagi waktu total dari semua client dilepas sampai semuanya selesai.
Tiap client adalah satu thread dengan koneksi gRPC sendiri; semua client dilepas bersamaan lewat barrier.

Pemakaian
  python benchmark.py                          jalankan server sendiri untuk 1 dan 4 worker, uji 1/5/20 client
  python benchmark.py --quick                  versi cepat untuk ditampilkan di video (sedikit gambar per client)
  python benchmark.py --workers 1 2 4          bandingkan jumlah worker lain
  python benchmark.py --target server:50051    uji server yang sudah berjalan (misalnya di Docker atau mesin lain)
  python benchmark.py --target IP:50051 --label "2 worker"   beri nama konfigurasi saat memakai --target

Keluaran (awalan berkas bisa diganti dengan --out)
  hasil_benchmark.csv   data mentah per konfigurasi dan jumlah client
  hasil_benchmark.md    tabel Markdown siap ditempel ke laporan
  hasil_benchmark.png   grafik latensi dan throughput (butuh matplotlib: pip install matplotlib)
"""
import argparse
import csv
import math
import os
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time

import grpc
from PIL import Image

import client as cl
import image_queue_pb2 as pb
import image_queue_pb2_grpc as pb_grpc

HERE = os.path.dirname(os.path.abspath(__file__))
POLL = 0.02            # detik antar pengecekan status (kecil agar tidak menambah latensi terukur)

# Warna grafik: dua slot pertama dari palet kategorikal tervalidasi (biru, oranye) di atas permukaan terang
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]


# ---------- pengukuran ----------
def make_test_image(path, size):
    """Gambar acak berwarna (tidak bisa dikompres): ukuran file besar dan kerja grayscale nyata."""
    Image.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3)).save(path, format="PNG")
    return os.path.getsize(path)


def percentile(sorted_values, p):
    """Persentil metode nearest-rank; aman untuk sampel kecil."""
    if not sorted_values:
        return float("nan")
    k = max(0, math.ceil(p / 100 * len(sorted_values)) - 1)
    return sorted_values[k]


def one_job(stub, path, client_id):
    """Satu gambar: upload sampai DONE. Mengembalikan (detik, berhasil)."""
    t0 = time.perf_counter()
    reply = cl.upload(stub, path, client_id)
    while True:
        r = stub.GetStatus(pb.StatusRequest(job_id=reply.job_id), timeout=cl.TIMEOUT)
        if r.status in (pb.DONE, pb.FAILED):
            return time.perf_counter() - t0, r.status == pb.DONE
        time.sleep(POLL)


def run_load(make_stub, n_clients, per_client, path):
    """Jalankan n_clients thread serentak, masing-masing mengirim per_client gambar. Mengembalikan (latensi_detik, galat, wall_detik)."""
    barrier = threading.Barrier(n_clients + 1)
    lats = [[] for _ in range(n_clients)]
    errors = [0] * n_clients

    def worker(i):
        stub = make_stub()
        barrier.wait()
        for _ in range(per_client):
            try:
                lat, ok = one_job(stub, path, f"bench-{i}")
                if ok:
                    lats[i].append(lat)
                else:
                    errors[i] += 1
            except Exception:  # noqa: BLE001 - dihitung sebagai galat
                errors[i] += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0
    return [x for per in lats for x in per], sum(errors), wall


def summarize(label, n_clients, per_client, lats, errors, wall):
    s = sorted(lats)
    ms = lambda v: round(v * 1000, 1)  # noqa: E731
    return {
        "konfigurasi": label,
        "client": n_clients,
        "gambar_dikirim": n_clients * per_client,
        "gambar_berhasil": len(lats),
        "galat": errors,
        "latensi_rata2_ms": ms(sum(s) / len(s)) if s else float("nan"),
        "latensi_p50_ms": ms(percentile(s, 50)),
        "latensi_p95_ms": ms(percentile(s, 95)),
        "latensi_maks_ms": ms(s[-1]) if s else float("nan"),
        "waktu_total_detik": round(wall, 2),
        "throughput_gambar_per_detik": round(len(lats) / wall, 2) if wall > 0 else float("nan"),
    }


# ---------- server ----------
def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(workers):
    port = free_port()
    code = f"import server; server.serve(port={port}, num_workers={workers})"
    err = tempfile.TemporaryFile()
    proc = subprocess.Popen([sys.executable, "-c", code], cwd=HERE, stdout=subprocess.DEVNULL, stderr=err)
    return proc, f"localhost:{port}", err


def stop_server(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def wait_ready(target, proc=None, err=None, timeout=20.0):
    stub = pb_grpc.ImageQueueStub(grpc.insecure_channel(target))
    t0 = time.time()
    while time.time() - t0 < timeout:
        if proc is not None and proc.poll() is not None:
            err.seek(0)
            tail = err.read().decode(errors="replace").strip().splitlines()[-3:]
            raise RuntimeError("server gagal start: " + (" | ".join(tail) or f"kode keluar {proc.returncode}"))
        try:
            stub.GetStatus(pb.StatusRequest(job_id="ping"), timeout=1)
            return
        except grpc.RpcError:
            time.sleep(0.2)
    raise RuntimeError(f"server di {target} tidak siap dalam {timeout:.0f} detik")


# ---------- keluaran ----------
def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def env_line():
    return f"Lingkungan uji: {os.cpu_count()} inti CPU logis, Python {platform.python_version()}, {platform.system()} {platform.release()}"


def markdown_table(rows):
    head = "| Konfigurasi | Client | Gambar berhasil | Galat | Latensi rata-rata (ms) | p50 (ms) | p95 (ms) | Throughput (gambar/detik) |"
    sep = "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    body = [f"| {r['konfigurasi']} | {r['client']} | {r['gambar_berhasil']}/{r['gambar_dikirim']} | {r['galat']} | "
            f"{r['latensi_rata2_ms']} | {r['latensi_p50_ms']} | {r['latensi_p95_ms']} | {r['throughput_gambar_per_detik']} |"
            for r in rows]
    return "\n".join([head, sep] + body)


def make_chart(rows, path):
    """Dua grafik garis berdampingan (bukan sumbu ganda): latensi dan throughput terhadap jumlah client."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  matplotlib belum terpasang, grafik dilewati. Pasang dengan: pip install matplotlib")
        return False

    labels = list(dict.fromkeys(r["konfigurasi"] for r in rows))
    clients = sorted({r["client"] for r in rows})
    xpos = {c: i for i, c in enumerate(clients)}
    plt.rcParams.update({"font.family": "sans-serif", "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                         "xtick.color": INK2, "ytick.color": INK2})
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8), dpi=160, facecolor=SURFACE)
    panels = [("Latensi per gambar (milidetik, lebih rendah lebih baik)", "latensi_rata2_ms"),
              ("Throughput (gambar per detik, lebih tinggi lebih baik)", "throughput_gambar_per_detik")]
    handles = []
    for ax, (title, key) in zip(axes, panels):
        ax.set_facecolor(SURFACE)
        for k, label in enumerate(labels):
            color = SERIES[k % len(SERIES)]
            pts = sorted((r for r in rows if r["konfigurasi"] == label), key=lambda r: r["client"])
            xs = [xpos[r["client"]] for r in pts]
            ys = [r[key] for r in pts]
            (line,) = ax.plot(xs, ys, color=color, lw=2, marker="o", ms=8, mec=SURFACE, mew=2, label=label, zorder=3)
            if ax is axes[0]:
                handles.append(line)
                up = [max(0.0, r["latensi_p95_ms"] - r["latensi_rata2_ms"]) for r in pts]   # garis tipis sampai p95
                ax.errorbar(xs, ys, yerr=[[0] * len(up), up], fmt="none", ecolor=color, elinewidth=1.2, capsize=4, alpha=.55, zorder=2)
            ax.annotate(label, xy=(xs[-1], ys[-1]), xytext=(9, 0), textcoords="offset points", va="center", fontsize=9, color=INK2)
        ax.set_xticks(range(len(clients)))
        ax.set_xticklabels([str(c) for c in clients])
        ax.set_xlim(-0.25, len(clients) - 1 + 0.65)
        ax.set_xlabel("Jumlah client serentak")
        ax.set_ylim(bottom=0)
        ax.set_title(title, loc="left", fontsize=10.5, color=INK, pad=12)
        ax.grid(axis="y", color=GRID, lw=.8, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.tick_params(length=0)
    if len(labels) >= 2:
        fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.005, 1.0), ncol=len(labels), frameon=False, fontsize=10, labelcolor=INK2)
    fig.text(0.01, 0.01, "Titik = rata-rata; garis tipis pada grafik latensi = sampai persentil ke-95.  " + env_line(),
             fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.035, 1, 0.92 if len(labels) >= 2 else 1))
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


def write_outputs(rows, prefix, chart=True):
    write_csv(rows, prefix + ".csv")
    with open(prefix + ".md", "w", encoding="utf-8") as f:
        f.write(markdown_table(rows) + "\n\n" + env_line() + "\n")
    made = make_chart(rows, prefix + ".png") if chart else False
    return made


# ---------- program utama ----------
def main(argv=None):
    ap = argparse.ArgumentParser(description="Uji kinerja antrian gambar gRPC")
    ap.add_argument("--clients", type=int, nargs="+", default=[1, 5, 20], help="jumlah client serentak (default 1 5 20)")
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 4], help="jumlah worker yang dibandingkan (default 1 4)")
    ap.add_argument("--per-client", type=int, default=10, help="gambar per client (default 10; --quick = 3)")
    ap.add_argument("--quick", action="store_true", help="versi cepat untuk video")
    ap.add_argument("--target", help="uji server yang sudah berjalan, alih-alih menjalankan server sendiri")
    ap.add_argument("--label", help="nama konfigurasi saat memakai --target")
    ap.add_argument("--size", default="800x600", help="ukuran gambar uji, LEBARxTINGGI (default 800x600)")
    ap.add_argument("--out", default="hasil_benchmark", help="awalan berkas keluaran")
    ap.add_argument("--no-chart", action="store_true", help="lewati pembuatan grafik")
    args = ap.parse_args(argv)

    per_client = 3 if args.quick and args.per_client == 10 else args.per_client
    try:
        w, h = (int(x) for x in args.size.lower().split("x"))
    except ValueError:
        print("Format --size salah, contoh: 800x600")
        return 2

    configs = [(args.label or "server", None)] if args.target else [(f"{n} worker", n) for n in args.workers]
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "uji.png")
        size_kb = make_test_image(path, (w, h)) // 1024
        print(f"Uji kinerja: {configs and [c[0] for c in configs]} x client {args.clients}, {per_client} gambar per client")
        print(f"Gambar uji {w}x{h}, {size_kb} KB ({math.ceil(size_kb * 1024 / cl.CHUNK_SIZE)} potongan). {env_line()}\n")
        for label, n_workers in configs:
            proc = err = None
            try:
                if args.target:
                    target = args.target
                else:
                    print(f"[{label}] menjalankan server...")
                    proc, target, err = start_server(n_workers)
                wait_ready(target, proc, err)
                make_stub = lambda t=target: pb_grpc.ImageQueueStub(grpc.insecure_channel(t))  # noqa: E731
                one_job(make_stub(), path, "bench-warmup")                                   # pemanasan, tidak dicatat
                for n in args.clients:
                    lats, errors, wall = run_load(make_stub, n, per_client, path)
                    row = summarize(label, n, per_client, lats, errors, wall)
                    rows.append(row)
                    print(f"  [{label}] {n:>2} client: latensi rata-rata {row['latensi_rata2_ms']} ms (p95 {row['latensi_p95_ms']}), "
                          f"throughput {row['throughput_gambar_per_detik']} gambar/detik, galat {row['galat']}")
            except RuntimeError as e:
                print(f"Gagal: {e}")
                return 2
            finally:
                stop_server(proc)

    print("\n" + markdown_table(rows) + "\n")
    chart = write_outputs(rows, args.out, chart=not args.no_chart)
    print(f"Tersimpan: {args.out}.csv, {args.out}.md" + (f", {args.out}.png" if chart else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
