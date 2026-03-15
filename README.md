Repositori ini berisi solusi end-to-end untuk migrasi data dan pembangunan datamart perusahaan Maju Jaya.

## Architecture Design (Task 3)
Solusi ini menggunakan **Medallion Architecture** untuk memastikan integritas data dari sumber mentah hingga laporan akhir.



- **Bronze (Raw):** Data asli dari MySQL & CSV harian.
- **Silver (Cleaned):** Data yang telah distandarisasi (format tanggal & pembersihan harga).
- **Gold (Datamart):** Tabel siap saji untuk Business Intelligence.

---

## Cara Menjalankan

### 1. Persiapan Infrastruktur
Pastikan Docker sudah terinstal, lalu jalankan:
```bash
docker-compose up -d

```
### 2. Buka jupyter notebook untuk running script nya atau running file main.py
