
# Antrian Gambar gRPC

Client mengunggah gambar per chunk. Server memasukkan pekerjaan ke antrian, memprosesnya dengan worker menjadi grayscale, lalu client mengunduh hasilnya.

## Menjalankan di Windows

Buka PowerShell pada folder project ini:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_queue.proto
```

Jika aktivasi virtual environment diblokir PowerShell, jalankan `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`, lalu aktifkan ulang.

Buka terminal pertama dan jalankan server:

```powershell
python server.py 2
```

Buka terminal kedua, aktifkan virtual environment yang sama, lalu jalankan client dengan path gambar milik Anda:

```powershell
python client.py "D:\path\foto.jpg" "D:\path\hasil.png"
```

Untuk menjalankan demo retry dan kegagalan server:

```powershell
python demo_b.py --pause
```

Server harus tetap berjalan saat client atau demo dijalankan. Angka `2` pada server adalah jumlah worker.

## Menjalankan dengan Docker

Pastikan Docker Desktop aktif, lalu jalankan dari folder yang berisi `docker-compose.yml`:

```powershell
docker compose up --build -d server
docker compose ps
docker compose run --rm client
docker compose logs -f server
docker compose down
```

Client container terhubung ke server melalui `server:50051`. Perintah `docker compose run --rm client` menjalankan demo B.

## RPC

| RPC | Jenis |
|---|---|
| UploadImage | Client streaming |
| GetStatus | Unary |
| DownloadResult | Server streaming |
| ListJobs | Unary |

Client mencoba ulang timeout dan server yang tidak tersedia hingga tiga kali. Upload mengirim `request_id` yang sama saat retry agar server tidak membuat job ganda.
