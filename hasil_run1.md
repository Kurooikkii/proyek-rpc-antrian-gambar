| Konfigurasi | Client | Gambar berhasil | Galat | Latensi rata-rata (ms) | p50 (ms) | p95 (ms) | Throughput (gambar/detik) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 worker | 1 | 10/10 | 0 | 26.0 | 26.0 | 26.6 | 38.42 |
| 1 worker | 5 | 50/50 | 0 | 107.7 | 111.5 | 116.6 | 44.8 |
| 1 worker | 20 | 200/200 | 0 | 392.2 | 403.1 | 422.8 | 48.69 |
| 4 worker | 1 | 10/10 | 0 | 25.8 | 25.6 | 26.7 | 38.76 |
| 4 worker | 5 | 50/50 | 0 | 47.3 | 49.2 | 60.4 | 99.38 |
| 4 worker | 20 | 200/200 | 0 | 165.5 | 164.4 | 186.8 | 117.31 |

Lingkungan uji: 12 inti CPU logis, Python 3.13.3, Windows 11
