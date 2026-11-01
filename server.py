"""Server antrian gambar (gRPC).

Alur: UploadImage (client streaming) -> antrian -> worker thread -> grayscale -> DONE.
"""
import io
import queue
import threading
import uuid
from concurrent import futures

import grpc
from PIL import Image

import image_queue_pb2 as pb
import image_queue_pb2_grpc as pb_grpc

MAX_IMAGE_BYTES = 5 * 1024 * 1024  # batas 5 MB per gambar
NUM_WORKERS = 2                     # ubah untuk eksperimen 1 vs 4 worker
CHUNK_SIZE = 64 * 1024              # ukuran chunk saat download


class ImageQueueService(pb_grpc.ImageQueueServicer):
    def __init__(self, num_workers=NUM_WORKERS):
        self.lock = threading.Lock()   # melindungi self.jobs dan self.requests
        self.jobs = {}       # job_id -> dict(client_id, filename, status, error, input, output)
        self.requests = {}   # request_id -> job_id (untuk cek duplikat / idempotensi)
        self.q = queue.Queue()
        for _ in range(num_workers):
            threading.Thread(target=self._worker, daemon=True).start()

    # ---------- RPC ----------
    def UploadImage(self, request_iterator, context):
        request_id, client_id, filename = None, "", ""
        buf = bytearray()
        for chunk in request_iterator:
            if request_id is None:
                request_id, client_id, filename = (
                    chunk.request_id, chunk.client_id, chunk.filename)
            buf.extend(chunk.data)
            if len(buf) > MAX_IMAGE_BYTES:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                              "Gambar melebihi batas 5 MB")
        if not request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id kosong")

        with self.lock:
            # Idempotensi: request_id yang sama (retry) -> kembalikan job lama
            if request_id in self.requests:
                return pb.UploadReply(job_id=self.requests[request_id], duplicate=True)
            job_id = uuid.uuid4().hex[:8]
            self.requests[request_id] = job_id
            self.jobs[job_id] = {
                "client_id": client_id, "filename": filename,
                "status": pb.QUEUED, "error": "",
                "input": bytes(buf), "output": b"",
            }
        self.q.put(job_id)
        return pb.UploadReply(job_id=job_id, duplicate=False)

    def GetStatus(self, request, context):
        with self.lock:
            job = self.jobs.get(request.job_id)
            if job is None:
                return pb.StatusReply(job_id=request.job_id, status=pb.UNKNOWN)
            return pb.StatusReply(job_id=request.job_id,
                                  status=job["status"], error=job["error"])

    def DownloadResult(self, request, context):
        with self.lock:
            job = self.jobs.get(request.job_id)
            if job is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "job_id tidak ditemukan")
            if job["status"] != pb.DONE:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "pekerjaan belum selesai")
            data = job["output"]
        for i in range(0, len(data), CHUNK_SIZE):
            yield pb.ResultChunk(data=data[i:i + CHUNK_SIZE])

    def ListJobs(self, request, context):
        with self.lock:
            infos = [pb.JobInfo(job_id=jid, filename=j["filename"], status=j["status"])
                     for jid, j in self.jobs.items()
                     if j["client_id"] == request.client_id]
        return pb.ListJobsReply(jobs=infos)

    # ---------- Worker ----------
    def _worker(self):
        while True:
            job_id = self.q.get()
            with self.lock:
                job = self.jobs[job_id]
                job["status"] = pb.PROCESSING
                data = job["input"]
            try:
                out = to_grayscale(data)   # proses berat dilakukan di luar lock
                with self.lock:
                    job["output"] = out
                    job["status"] = pb.DONE
                    job["input"] = b""
            except Exception as e:  # gambar rusak / format tidak didukung
                with self.lock:
                    job["status"] = pb.FAILED
                    job["error"] = str(e)
            finally:
                self.q.task_done()


def to_grayscale(data: bytes) -> bytes:
    img = Image.open(io.BytesIO(data)).convert("L")
    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def serve(port=50051, num_workers=NUM_WORKERS):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_ImageQueueServicer_to_server(ImageQueueService(num_workers), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server berjalan di port {port} dengan {num_workers} worker")
    server.wait_for_termination()


if __name__ == "__main__":
    import sys
    serve(num_workers=int(sys.argv[1]) if len(sys.argv) > 1 else NUM_WORKERS)
