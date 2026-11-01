"""Client antrian gambar: upload -> polling status -> download hasil.

Pemakaian: python client.py <gambar_input> <gambar_output> [host:port]
"""
import sys
import time
import uuid

import grpc

import image_queue_pb2 as pb
import image_queue_pb2_grpc as pb_grpc

CHUNK_SIZE = 64 * 1024
TIMEOUT = 5.0       # detik per panggilan
MAX_RETRY = 3


def with_retry(fn):
    """Jalankan fn(); ulangi bila timeout/server tidak tersedia."""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            return fn()
        except grpc.RpcError as e:
            retryable = e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED,
                                     grpc.StatusCode.UNAVAILABLE)
            if not retryable or attempt == MAX_RETRY:
                raise
            time.sleep(0.5 * attempt)   # TODO: coba exponential backoff


def upload(stub, path, client_id, request_id=None):
    request_id = request_id or uuid.uuid4().hex   # SAMA di setiap retry

    def gen():
        with open(path, "rb") as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                yield pb.UploadChunk(request_id=request_id, client_id=client_id,
                                     filename=path, data=data)

    return with_retry(lambda: stub.UploadImage(gen(), timeout=TIMEOUT))


def wait_done(stub, job_id, poll=0.2, limit=60):
    start = time.time()
    while time.time() - start < limit:
        r = with_retry(lambda: stub.GetStatus(pb.StatusRequest(job_id=job_id),
                                              timeout=TIMEOUT))
        if r.status in (pb.DONE, pb.FAILED):
            return r
        time.sleep(poll)
    raise TimeoutError("pekerjaan tidak selesai tepat waktu")


def download(stub, job_id, out_path):
    def call():
        return b"".join(c.data for c in
                        stub.DownloadResult(pb.DownloadRequest(job_id=job_id),
                                            timeout=TIMEOUT))
    data = with_retry(call)
    with open(out_path, "wb") as f:
        f.write(data)


if __name__ == "__main__":
    src, dst = sys.argv[1], sys.argv[2]
    target = sys.argv[3] if len(sys.argv) > 3 else "localhost:50051"
    with grpc.insecure_channel(target) as channel:
        stub = pb_grpc.ImageQueueStub(channel)
        reply = upload(stub, src, client_id="client-1")
        print("job_id:", reply.job_id, "(duplikat)" if reply.duplicate else "")
        status = wait_done(stub, reply.job_id)
        print("status:", pb.JobStatus.Name(status.status), status.error)
        if status.status == pb.DONE:
            download(stub, reply.job_id, dst)
            print("hasil disimpan di", dst)
