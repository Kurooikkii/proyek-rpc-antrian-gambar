
# Antrian Gambar gRPC (Studi Kasus 5)

Upload gambar per chunk -> antrian -> worker ubah ke grayscale -> cek status -> download hasil.

## Instalasi (tanpa Docker)
```bash
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_queue.proto
```
Perintah kedua menghasilkan stub (`image_queue_pb2.py` dan `image_queue_pb2_grpc.py`).

## Menjalankan
```bash
python server.py 2                                    # server dengan 2 worker (ubah angkanya untuk eksperimen)
python client.py foto.jpg hasil.png                   # client baris perintah (terminal lain)
python client.py foto.jpg hasil.png IP_SERVER:50051   # dari komputer lain
python web_ui.py                                      # antarmuka web Prinko di http://localhost:8000
```
Antarmuka web terhubung ke `localhost:50051`; untuk server lain isi variabel `GRPC_SERVER`, misalnya `GRPC_SERVER=IP_SERVER:50051`.

## Menjalankan dengan Docker (2 container terpisah)
```bash
docker compose up --build -d server web     # container server dan antarmuka web
docker compose run --rm client              # container client menjalankan demo B terhadap container server
docker compose logs -f server               # log server
docker compose down                         # matikan semua
```
Di dalam jaringan Docker, client memanggil server lewat nama layanannya (`server:50051`). Antarmuka web dibuka di http://localhost:8000.

## RPC
| RPC | Jenis |
|---|---|
| UploadImage | client streaming |
| GetStatus | unary |
| DownloadResult | server streaming |
| ListJobs | unary |

## Toleransi kegagalan di client
- **Timeout:** tiap panggilan dibatasi `TIMEOUT = 5` detik.
- **Retry:** maksimal `MAX_RETRY = 3` percobaan untuk `DEADLINE_EXCEEDED` dan `UNAVAILABLE`, dengan exponential backoff (0,5 detik, 1 detik, ...) ditambah sedikit jitter. Error permanen tidak diulang.
- **Aman diulang:** upload memakai `request_id` yang sama di setiap percobaan, sehingga server tidak membuat pekerjaan ganda.

## Skrip demo untuk video
```bash
python demo_a.py --pause    # jobdesk A: server, concurrency, idempotensi, kegagalan
python demo_b.py --pause    # jobdesk B: client, retry, server tidak terjangkau
```
Server harus sudah berjalan. Tambahkan `--scene N` untuk mengulang satu adegan.

## Yang masih perlu dikerjakan kelompok
- [ ] Skrip uji kinerja (`benchmark.py`): latensi dan throughput untuk 1, 5, 20 client
- [ ] Eksperimen 1 worker vs 4 worker, tabel dan grafik
- [ ] Laporan PDF dan video demo
