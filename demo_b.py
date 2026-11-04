"""Skrip demo jobdesk B (client, timeout, retry, Docker) untuk video demo.

Menjalankan 3 adegan berurutan dan mencetak LULUS atau GAGAL untuk tiap pemeriksaan:
  1. Client: upload per chunk, polling status, download
  2. Retry: balasan server hilang, request_id yang sama mencegah job ganda
  3. Server tidak terjangkau: exponential backoff lalu menyerah dengan rapi

Adegan 4 (Docker dan antarmuka web) dilakukan manual, lihat dokumen naskah.

Server harus sudah berjalan lebih dulu (python server.py 2, atau docker compose up -d server).

Pemakaian:
  python demo_b.py                       semua adegan
  python demo_b.py --pause               berhenti tiap adegan, tekan Enter untuk lanjut (enak untuk merekam)
  python demo_b.py --scene 2             ulang satu adegan saja
  python demo_b.py server:50051          alamat server lain (misalnya nama layanan di Docker)
"""
import argparse
import math
import os
import sys
import tempfile
import time
import uuid

import grpc
from PIL import Image

import client as cl
import image_queue_pb2 as pb
import image_queue_pb2_grpc as pb_grpc

DEAD_TARGET = "localhost:59999"     # alamat tempat tidak ada server (untuk adegan 3)

if os.name == "nt":
    os.system("")                   # mengaktifkan warna ANSI di terminal Windows

_C = {"g": "\033[92m", "r": "\033[91m", "y": "\033[93m", "c": "\033[96m",
      "b": "\033[1m", "d": "\033[2m", "0": "\033[0m"}


def col(k, text):
    return f"{_C[k]}{text}{_C['0']}"


def noise_png(path, size=(600, 400)):
    """Gambar acak berwarna (tidak bisa dikompres), supaya ukurannya cukup besar untuk beberapa chunk."""
    Image.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3)).save(path, format="PNG")
    return path


class InjectedError(grpc.RpcError):
    """Kegagalan jaringan buatan, dipakai demo untuk meniru balasan yang hilang."""

    def __init__(self, code):
        Exception.__init__(self)
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return "kegagalan jaringan buatan (disuntikkan oleh demo)"


class LostReplyStub:
    """Pembungkus stub: balasan upload pertama sengaja 'hilang' SESUDAH server selesai memprosesnya."""

    def __init__(self, real, lose=1, code=grpc.StatusCode.DEADLINE_EXCEEDED):
        self.real, self.lose, self.code, self.calls = real, lose, code, 0

    def UploadImage(self, it, timeout=None):
        self.calls += 1
        reply = self.real.UploadImage(it, timeout=timeout)        # server benar-benar memproses kiriman ini
        if self.calls <= self.lose:
            raise InjectedError(self.code)                         # tetapi balasannya tidak sampai ke client
        return reply

    def __getattr__(self, name):
        return getattr(self.real, name)


class CountingStub:
    """Pembungkus stub yang menghitung berapa kali UploadImage dipanggil."""

    def __init__(self, real):
        self.real, self.calls = real, 0

    def UploadImage(self, it, timeout=None):
        self.calls += 1
        return self.real.UploadImage(it, timeout=timeout)

    def __getattr__(self, name):
        return getattr(self.real, name)


class DemoB:
    def __init__(self, stub, tmpdir, pause=False):
        self.stub, self.tmp, self.pause = stub, tmpdir, pause
        self.base = "demoB-" + uuid.uuid4().hex[:6]
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

    def count_jobs(self, client_id):
        r = self.stub.ListJobs(pb.ListJobsRequest(client_id=client_id), timeout=cl.TIMEOUT)
        return len(r.jobs)

    # ---------- adegan ----------
    def scene1(self):
        self.banner(1, "Client: upload per chunk, polling status, download")
        src = noise_png(os.path.join(self.tmp, "asli.png"))
        size = os.path.getsize(src)
        chunks = math.ceil(size / cl.CHUNK_SIZE)
        self.info(f"Gambar {size // 1024} KB dikirim dalam {chunks} potongan @ {cl.CHUNK_SIZE // 1024} KB")
        t0 = time.time()
        reply = cl.upload(self.stub, src, self.base + "-s1")
        self.info(f"Server membalas job_id = {col('b', reply.job_id)}")
        status = cl.wait_done(self.stub, reply.job_id)
        dst = os.path.join(self.tmp, "hasil.png")
        cl.download(self.stub, reply.job_id, dst)
        self.info(f"Hasil disimpan ke {dst} ({time.time() - t0:.2f} detik total)")
        self.check("Gambar dikirim lebih dari satu potongan", chunks > 1, f"{chunks} potongan")
        self.check("Status akhir DONE", status.status == pb.DONE)
        out = Image.open(dst)
        self.check("Hasil unduhan berupa gambar grayscale", out.mode == "L", f"mode {out.mode}")
        self.check("Ukuran hasil sama dengan aslinya", out.size == (600, 400), f"{out.size[0]}x{out.size[1]}")

    def scene2(self):
        self.banner(2, "Retry: balasan server hilang, request_id yang sama mencegah job ganda")
        cid = self.base + "-s2"
        src = noise_png(os.path.join(self.tmp, "kecil.png"), (120, 80))
        flaky = LostReplyStub(self.stub, lose=1)
        self.info("Upload pertama: server memproses, tetapi balasannya sengaja dibuat hilang (timeout)...")
        try:
            reply = cl.upload(flaky, src, cid)
        except grpc.RpcError as e:
            self.check("Client pulih lewat retry", False, e.code().name)
            return
        self.info(f"Balasan akhir: job {col('b', reply.job_id)}, duplikat={reply.duplicate}")
        self.check("Client mengulang kiriman secara otomatis", flaky.calls == 2, f"{flaky.calls} percobaan")
        self.check("Server mengenali kiriman ulang (duplikat=True)", reply.duplicate)
        n = self.count_jobs(cid)
        self.check("Hanya 1 job tercatat, bukan 2", n == 1, f"{n} tercatat")
        r = cl.wait_done(self.stub, reply.job_id)
        self.check("Job tetap selesai (DONE)", r.status == pb.DONE)

    def scene3(self):
        self.banner(3, "Server tidak terjangkau: backoff, lalu menyerah dengan rapi")
        cid = self.base + "-s3"
        src = noise_png(os.path.join(self.tmp, "kecil3.png"), (120, 80))
        dead = CountingStub(pb_grpc.ImageQueueStub(grpc.insecure_channel(DEAD_TARGET)))
        self.info(f"Mengirim ke {DEAD_TARGET}, di alamat itu tidak ada server...")
        t0 = time.time()
        try:
            cl.upload(dead, src, cid)
            self.check("Client melaporkan kegagalan", False, "tidak ada error padahal server tidak ada")
        except grpc.RpcError as e:
            elapsed = time.time() - t0
            min_wait = sum(cl.BASE_DELAY * 2 ** i for i in range(cl.MAX_RETRY - 1))
            self.check("Error UNAVAILABLE diterima client", e.code() == grpc.StatusCode.UNAVAILABLE, e.code().name)
            self.check(f"Mencoba tepat {cl.MAX_RETRY} kali sebelum menyerah", dead.calls == cl.MAX_RETRY,
                       f"{dead.calls} percobaan")
            self.check("Jeda antar percobaan berlipat ganda (backoff)", elapsed >= min_wait,
                       f"{elapsed:.1f} detik, minimal {min_wait:.1f} detik")
        self.info("Sekarang kirim ulang dari client yang sama ke server yang hidup...")
        reply = cl.upload(self.stub, src, cid)
        r = cl.wait_done(self.stub, reply.job_id)
        self.check("Setelah server tersedia, upload berhasil (DONE)", r.status == pb.DONE)

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
    ap = argparse.ArgumentParser(description="Demo jobdesk B (client, retry, timeout)")
    ap.add_argument("target", nargs="?", default=os.environ.get("GRPC_SERVER", "localhost:50051"),
                    help="alamat server (default localhost:50051, atau isi GRPC_SERVER)")
    ap.add_argument("--pause", action="store_true", help="berhenti di akhir tiap adegan sampai Enter ditekan")
    ap.add_argument("--scene", type=int, choices=range(1, 4), help="jalankan satu adegan saja")
    args = ap.parse_args(argv)

    stub = pb_grpc.ImageQueueStub(grpc.insecure_channel(args.target))
    try:
        stub.GetStatus(pb.StatusRequest(job_id="ping"), timeout=3)
    except grpc.RpcError as e:
        print(col("r", f"Server tidak terjangkau di {args.target} ({e.code().name})."))
        print("Jalankan dulu:  python server.py 2   (atau: docker compose up -d server)")
        return 2

    with tempfile.TemporaryDirectory() as tmpdir:
        demo = DemoB(stub, tmpdir, pause=args.pause)
        scenes = [demo.scene1, demo.scene2, demo.scene3]
        chosen = [scenes[args.scene - 1]] if args.scene else scenes
        print(col("b", f"\nDemo jobdesk B  |  server: {args.target}  |  client_id: {demo.base}"))
        for scene in chosen:
            scene()
            demo.wait_enter()
        return 0 if demo.summary() else 1


if __name__ == "__main__":
    sys.exit(main())
