FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Buat stub gRPC dari file .proto saat image dibangun
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_queue.proto

EXPOSE 50051 8000

# Perintah bawaan: jalankan server dengan 2 worker (diganti per layanan di docker-compose.yml)
CMD ["python", "server.py", "2"]
