## IS402.P21-Cloud

# Bài toán: Xây dựng hệ thống xử lý dữ liệu thời gian thực cho cửa hàng bán lẻ.

# Mục tiêu:
- Thiết kế và triển khai hệ thống xử lý dữ liệu thời gian thực nhằm hỗ trợ thu thập, tổng hợp, phân tích và hiện thị thông tin hoạt động một cách nhanh chóng, chính xác.
- Hệ thống có khả năng mở rộng, độ tin cậy cao, hiệu suất tốt, ổn định và hỗ trợ trực quan hóa dữ liệu cho hoạt động kinh doanh.

# Kiến trúc hệ thống:
- `Apache Kafka`: Đóng vai trò truyền tải dữ liệu trong hệ thống, đảm nhiệm vai trò tiếp nhận và phân phối kết quả kinh doanh từ các chi nhánh tới hệ thống phân tích. 
- `Apache Flink`: Sử dụng cho mục đích xử lý, phân tích dữ liệu theo thời gian thực.
- `PostgreSQL`: Lưu trữ kết quả cho quá trình xử lý dữ liệu, phục vụ truy vấn và khai thác dữ liệu, đảm bảo tính toàn vẹn dữ liệu và hiệu suất cao khi kết hợp các công cụ phân tích.
- `Metabase`: Tạo báo cáo, biểu đồ thống kê kết nối trực tiếp với PostgreSQL.

# Công nghệ và ngôn ngữ lập trình
- `Java`: Phát triển ứng dụng nhận và phân tích, lưu trữ dữ liệu.
- `Python`: Xây dựng hệ thống gửi dữ liệu.
- `SQL`: Truy vấn phục vụ cho quá trình trực quan hoá và báo cáo dữ liệu.
- `Docker`: Cài đặt môi trường, đóng gói các ứng dụng của hệ thống, dễ dàng cho quá trình phát triển, sử dụng và quản lý.
