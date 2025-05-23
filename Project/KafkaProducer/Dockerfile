# Sử dụng image Python 3.10 làm base image
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép các file từ thư mục hiện tại vào thư mục làm việc trong container
COPY . .

# Cài đặt các thư viện cần thiết từ file requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Chạy file Python khi container được khởi động
CMD ["python", "Producer_threading.py"]
