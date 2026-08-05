# ใช้ Python 3.11 เป็น Base Image
FROM python:3.11-slim

# ตั้งค่า Working Directory
WORKDIR /app

# ติดตั้ง System Dependencies ที่จำเป็น (เช่น gcc สำหรับ build C libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# คัดลอกและติดตั้ง Python Libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอกซอร์สโค้ดในโปรเจกต์
COPY . .

# เปิด พอร์ตสำหรับ Jupyter Notebook
EXPOSE 8888

# คำสั่งเปิดใช้งาน Jupyter Lab โดยไม่ถาม รหัสผ่าน (สำหรับ Local Development)
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]