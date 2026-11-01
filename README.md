# Antrian Gambar gRPC (Studi Kasus 5)

Upload gambar per chunk -> antrian -> worker ubah ke grayscale -> cek status -> download hasil.

## Instalasi
```bash
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_queue.proto
```
Perintah kedua menghasilkan stub (`image_queue_pb2.py` dan `image_queue_pb2_grpc.py`).

## Menjalankan
```bash
python server.py 2                      # 2 worker (ubah angkanya untuk eksperimen)
python client.py foto.jpg hasil.png     # terminal lain
python client.py foto.jpg hasil.png IP_SERVER:50051   # dari komputer/container lain
```

## RPC
| RPC | Jenis |
|---|---|
| UploadImage | client streaming |
| GetStatus | unary |
| DownloadResult | server streaming |
| ListJobs | unary |

## Yang masih perlu dikerjakan kelompok
- [ ] Docker: `Dockerfile` dan `docker-compose.yml` (server dan client di container berbeda)
- [ ] Skrip uji kinerja (`benchmark.py`): latensi dan throughput untuk 1, 5, 20 client
- [ ] Uji kegagalan: matikan server di tengah proses, buktikan retry tidak membuat job ganda
- [ ] Eksperimen 1 worker vs 4 worker, tabel dan grafik
- [ ] Laporan PDF dan video demo
