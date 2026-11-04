"""Skrip demo jobdesk A (server dan .proto) untuk video demo.

Menjalankan 5 adegan berurutan dan mencetak LULUS atau GAGAL untuk tiap pemeriksaan:
  1. Desain antarmuka (.proto)
  2. Alur lengkap: upload -> antrean -> grayscale -> download
  3. Concurrency: 20 client serentak
  4. Idempotensi: request_id yang sama tidak membuat job baru
  5. Toleransi kegagalan: file rusak dan file terlalu besar

Server harus sudah berjalan lebih dulu:   python server.py 2

Pemakaian:
  python demo_a.py                       semua adegan
  python demo_a.py --pause               berhenti tiap adegan, tekan Enter untuk lanjut (enak untuk merekam)
  python demo_a.py --scene 4             ulang satu adegan saja (untuk take ulang)
  python demo_a.py 192.168.1.10:50051    server di komputer atau container lain
"""
import argparse
import io
import os
import re
import sys
import threading
import time
import uuid

import grpc
from PIL import Image

import image_queue_pb2 as pb
import image_queue_pb2_grpc as pb_grpc

CHUNK = 64 * 1024
MAX_MB = 5            # harus sama dengan MAX_IMAGE_BYTES di server.py
TIMEOUT = 30.0
N_CLIENT = 20
HERE = os.path.dirname(os.path.abspath(__file__))

if os.name == "nt":
    os.system("")     # mengaktifkan warna ANSI di terminal Windows

_C = {"g": "\033[92m", "r": "\033[91m", "y": "\033[93m", "c": "\033[96m",
      "b": "\033[1m", "d": "\033[2m", "0": "\033[0m"}


def col(k, text):
    return f"{_C[k]}{text}{_C['0']}"


def make_image(seed, size=(240, 150)):
    """Gambar berwarna buatan sendiri (gradasi), supaya demo tidak butuh file luar."""
    w, h = size
    im = Image.new("RGB", size)
    px = im.load()
    for x in range(w):
        for y in range(h):
            px[x, y] = ((x * 255 // w + seed * 37) % 256,
                        (y * 255 // h + seed * 91) % 256,
                        (255 - x * 255 // w + seed * 53) % 256)
    out = io.BytesIO()
    im.save(out, format="PNG")
    return out.getvalue()


class Demo:
    def __init__(self, stub, pause=False):
        self.stub = stub
        self.pause = pause
        self.base = "demoA-" + uuid.uuid4().hex[:6]   # client_id unik tiap kali demo dijalankan
        self.results = []

    # ---------- alat bantu tampilan ----------
    def banner(self, n, title):
        line = col("c", "=" * 66)
        print(f"\n{line}\n{col('b', f' ADEGAN {n}: {title}')}\n{line}")

    def info(self, text):
        print(f"  {col('y', '>')} {text}")

    def check(self, label, ok, detail=""):
        self.results.append((label, bool(ok)))
        tag = col("g", "[LULUS]") if ok else col("r", "[GAGAL]")
        extra = col("d", f"  ({detail})") if detail else ""
        print(f"  {tag} {label}{extra}")
        return ok

    def wait_enter(self):
        if self.pause:
            input(col("d", "\n  [Enter] untuk lanjut ke adegan berikutnya... "))

    # ---------- alat bantu RPC ----------
    def upload(self, data, request_id, client_id, name="gambar.png"):
        def gen():
            for i in range(0, max(len(data), 1), CHUNK):
                yield pb.UploadChunk(request_id=request_id, client_id=client_id,
                                     filename=name, data=data[i:i + CHUNK])
        return self.stub.UploadImage(gen(), timeout=TIMEOUT)

    def wait_done(self, job_id, limit=30.0):
        seen, t0 = [], time.time()
        while time.time() - t0 < limit:
            r = self.stub.GetStatus(pb.StatusRequest(job_id=job_id), timeout=TIMEOUT)
            name = pb.JobStatus.Name(r.status)
            if not seen or seen[-1] != name:
                seen.append(name)
            if r.status in (pb.DONE, pb.FAILED):
                return r, seen
            time.sleep(0.05)
        raise TimeoutError(f"job {job_id} tidak selesai dalam {limit} detik")

    def download(self, job_id):
        stream = self.stub.DownloadResult(pb.DownloadRequest(job_id=job_id), timeout=TIMEOUT)
        return b"".join(c.data for c in stream)

    def count_jobs(self, client_id):
        r = self.stub.ListJobs(pb.ListJobsRequest(client_id=client_id), timeout=TIMEOUT)
        return len(r.jobs)

    def run_threads(self, n, fn):
        """Jalankan fn(i) di n thread yang dilepas bersamaan lewat barrier."""
        barrier, out, errs = threading.Barrier(n), [None] * n, []

        def work(i):
            try:
                barrier.wait()
                out[i] = fn(i)
            except Exception as e:  # noqa: BLE001 - dicatat dan dilaporkan
                errs.append(repr(e))

        threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return out, errs, time.time() - t0

    # ---------- adegan ----------
    def scene1(self):
        self.banner(1, "Desain antarmuka (.proto)")
        path = os.path.join(HERE, "image_queue.proto")
        if not os.path.exists(path):
            self.check("File image_queue.proto ditemukan", False, path)
            return
        with open(path, encoding="utf-8") as f:
            text = f.read()
        rpcs = re.findall(r"rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s*returns\s*\(\s*(stream\s+)?(\w+)\s*\)", text)
        kinds = []
        for name, s_in, req, s_out, resp in rpcs:
            if s_in and s_out:
                kind = "bidirectional streaming"
            elif s_in:
                kind = "client streaming"
            elif s_out:
                kind = "server streaming"
            else:
                kind = "unary"
            kinds.append(kind)
            print(f"    {col('b', name):<30} {req} -> {resp:<14} [{kind}]")
        self.check("Minimal 4 RPC", len(rpcs) >= 4, f"{len(rpcs)} RPC")
        self.check("Ada RPC streaming", any("streaming" in k for k in kinds),
                   ", ".join(sorted({k for k in kinds if 'streaming' in k})))

    def scene2(self):
        self.banner(2, "Alur lengkap: upload -> antrean -> grayscale -> download")
        cid = self.base + "-s2"
        data = make_image(1)
        self.info(f"Mengirim 1 gambar berwarna ({len(data)} byte) per chunk {CHUNK // 1024} KB...")
        t0 = time.time()
        reply = self.upload(data, uuid.uuid4().hex, cid)
        self.info(f"Server membalas job_id = {col('b', reply.job_id)}")
        r, seen = self.wait_done(reply.job_id)
        elapsed = time.time() - t0
        self.info("Status yang teramati: " + " -> ".join(seen))
        self.check("Status akhir DONE", r.status == pb.DONE, f"{elapsed:.2f} detik dari upload sampai selesai")
        out = self.download(reply.job_id)
        img = Image.open(io.BytesIO(out))
        self.check("Hasil unduhan berupa gambar grayscale", img.mode == "L", f"mode {img.mode}")
        self.check("Ukuran gambar hasil sama dengan aslinya", img.size == (240, 150), f"{img.size[0]}x{img.size[1]}")

    def scene3(self):
        self.banner(3, f"Concurrency: {N_CLIENT} client mengirim serentak")
        cid = self.base + "-s3"
        images = [make_image(i) for i in range(N_CLIENT)]
        self.info(f"{N_CLIENT} thread dilepas bersamaan, masing-masing mengunggah gambar berbeda...")

        def one(i):
            reply = self.upload(images[i], uuid.uuid4().hex, cid, f"foto{i}.png")
            r, _ = self.wait_done(reply.job_id)
            return reply.job_id, r.status

        out, errs, secs = self.run_threads(N_CLIENT, one)
        done = [o for o in out if o]
        ids = {o[0] for o in done}
        self.info(f"Selesai dalam {secs:.2f} detik ({N_CLIENT / secs:.1f} gambar per detik)")
        self.check("Tidak ada error di sisi client", not errs, errs[0] if errs else "")
        self.check(f"{N_CLIENT} job_id berbeda semua", len(ids) == N_CLIENT, f"{len(ids)} unik")
        self.check("Semua pekerjaan berstatus DONE", len(done) == N_CLIENT and all(s == pb.DONE for _, s in done))
        n = self.count_jobs(cid)
        self.check(f"ListJobs mencatat tepat {N_CLIENT} pekerjaan", n == N_CLIENT, f"{n} tercatat")

    def scene4(self):
        self.banner(4, "Idempotensi: request_id yang sama tidak membuat job baru")
        cid = self.base + "-s4"
        data = make_image(7)

        rid = uuid.uuid4().hex
        self.info("Mengirim gambar yang sama dua kali dengan request_id yang sama (meniru retry)...")
        a = self.upload(data, rid, cid)
        b = self.upload(data, rid, cid)
        self.info(f"Kiriman 1 -> job {a.job_id} (duplikat={a.duplicate}); kiriman 2 -> job {b.job_id} (duplikat={b.duplicate})")
        self.check("Kedua balasan memuat job_id yang sama", a.job_id == b.job_id)
        self.check("Kiriman pertama baru, kiriman kedua ditandai duplikat", (not a.duplicate) and b.duplicate)
        self.wait_done(a.job_id)

        rid2 = uuid.uuid4().hex
        self.info(f"Sekarang {N_CLIENT} thread mengirim request_id yang sama pada saat yang sama...")

        def same(_i):
            rep = self.upload(data, rid2, cid)
            return rep.job_id, rep.duplicate

        out, errs, _ = self.run_threads(N_CLIENT, same)
        got = [o for o in out if o]
        ids = {o[0] for o in got}
        fresh = [o for o in got if not o[1]]
        self.check("Tidak ada error di sisi client", not errs, errs[0] if errs else "")
        self.check(f"{N_CLIENT} kiriman serentak hanya menghasilkan 1 job", len(ids) == 1, f"{len(ids)} job_id unik")
        self.check("Tepat satu kiriman yang diproses, sisanya duplikat", len(fresh) == 1,
                   f"{len(fresh)} baru, {len(got) - len(fresh)} duplikat")
        if ids:
            self.wait_done(next(iter(ids)))
        n = self.count_jobs(cid)
        self.check("Total pekerjaan tercatat hanya 2 (bukan 22)", n == 2, f"{n} tercatat")

    def scene5(self):
        self.banner(5, "Toleransi kegagalan: file rusak dan file terlalu besar")
        cid = self.base + "-s5"

        self.info("Mengirim file yang bukan gambar (isinya teks biasa)...")
        reply = self.upload(b"ini bukan gambar, hanya teks biasa", uuid.uuid4().hex, cid, "rusak.png")
        r, _ = self.wait_done(reply.job_id)
        self.check("Pekerjaan berstatus FAILED, server tidak mogok", r.status == pb.FAILED)
        self.check("Pesan error diteruskan ke client", bool(r.error), (r.error or "")[:60])

        self.info(f"Mengirim file {MAX_MB + 0.5:.1f} MB (batas server {MAX_MB} MB)...")
        big = os.urandom(int((MAX_MB + 0.5) * 1024 * 1024))
        try:
            self.upload(big, uuid.uuid4().hex, cid, "besar.bin")
            self.check("File terlalu besar ditolak", False, "server menerima file besar")
        except grpc.RpcError as e:
            self.check("File terlalu besar ditolak (RESOURCE_EXHAUSTED)",
                       e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED, f"{e.code().name}: {e.details()}")
        self.check("File besar tidak masuk antrean", self.count_jobs(cid) == 1, "hanya file rusak yang tercatat")

        self.info("Menanyakan status job_id yang tidak ada...")
        r = self.stub.GetStatus(pb.StatusRequest(job_id="tidak-ada"), timeout=TIMEOUT)
        self.check("Job_id asing dijawab UNKNOWN, bukan crash", r.status == pb.UNKNOWN)

        self.info("Memastikan server masih sehat setelah semua gangguan tadi...")
        ok = self.upload(make_image(3), uuid.uuid4().hex, cid)
        r, _ = self.wait_done(ok.job_id)
        self.check("Upload normal sesudahnya tetap berhasil (DONE)", r.status == pb.DONE)

    def summary(self):
        total = len(self.results)
        passed = sum(1 for _, ok in self.results if ok)
        line = col("c", "=" * 66)
        print(f"\n{line}\n{col('b', ' RINGKASAN')}\n{line}")
        for label, ok in self.results:
            tag = col("g", "LULUS") if ok else col("r", "GAGAL")
            print(f"  {tag}  {label}")
        color = "g" if passed == total else "r"
        print("\n  " + col(color, f"{passed} dari {total} pemeriksaan lulus"))
        return passed == total


def main(argv=None):
    ap = argparse.ArgumentParser(description="Demo jobdesk A (server dan .proto)")
    ap.add_argument("target", nargs="?", default="localhost:50051", help="alamat server (default localhost:50051)")
    ap.add_argument("--pause", action="store_true", help="berhenti di akhir tiap adegan sampai Enter ditekan")
    ap.add_argument("--scene", type=int, choices=range(1, 6), help="jalankan satu adegan saja")
    args = ap.parse_args(argv)

    stub = pb_grpc.ImageQueueStub(grpc.insecure_channel(args.target))
    try:
        stub.GetStatus(pb.StatusRequest(job_id="ping"), timeout=3)
    except grpc.RpcError as e:
        print(col("r", f"Server tidak terjangkau di {args.target} ({e.code().name})."))
        print("Jalankan dulu di terminal lain:  python server.py 2")
        return 2

    demo = Demo(stub, pause=args.pause)
    scenes = [demo.scene1, demo.scene2, demo.scene3, demo.scene4, demo.scene5]
    chosen = [scenes[args.scene - 1]] if args.scene else scenes
    print(col("b", f"\nDemo jobdesk A  |  server: {args.target}  |  client_id: {demo.base}"))
    for scene in chosen:
        scene()
        demo.wait_enter()
    return 0 if demo.summary() else 1


if __name__ == "__main__":
    sys.exit(main())
