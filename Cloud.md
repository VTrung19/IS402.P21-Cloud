# IS402.P21-Cloud

## Yêu cầu
- Ubuntu 22.04 LTS.
- Python 3.10.12.
- Java 11.
- Maven 3.6.3.
- Docker.


## Cài đặt và sử dụng Kafka
- Cài đặt: https://www.devopshint.com/how-to-install-apache-kafka-on-ubuntu-22-04-lts/
- Khởi tạo zookeeper và kafka:
    - `sudo systemctl start zookeeper.service kafka.service`
- Thay `status`|`stop` để kiểm tra trạng thái hoạt động hoặc dừng.
- Truy cập folder Kafka
    - Xóa topic: `bin/kafka-topics.sh --delete --topic InvoiceTopic --bootstrap-server localhost:9092`
    - Tạo topic: `bin/kafka-topics.sh --create --topic InvoiceTopic --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1`
    - Xem các message trong topic: `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic InvoiceTopic --from-beginning`


## Cài đặt và sử dụng Flink
- Cài đặt Apache Flink 1.17.0 Binaries `https://flink.apache.org/downloads/`:
  - Giải nén: `tar xzf flink-1.17.0-bin-scala_2.12.tgz`.
  - Di chuyển qua nơi lưu trữ `mv flink-1.17.0-bin-scala_2.12 /opt/flink`. 
  - Đối với trường hợp thì cần cấu hình lại Flink `conf/flink-conf.yaml`, cấu hình tùy theo số lượng mong muốn `taskmanager.numberOfTaskSlots: 2`.
  - Khởi tạo Flink: `bin/start-cluster.sh`.
  - Kết thúc Flink: `bin/stop-cluster.sh`.
  - Truy cập web dashboard: `http://localhost:8081/`.


## Cài đặt và sử dụng PostgreSQL
- Thực hiện `docker compose up --build -d` tạo ra container cho PostgreSQL tự động tạo cơ sở dữ liệu **Revenue**, các bảng dữ liệu khác.
- Các thao tác khác:
  - Xem danh sách container đang chạy `sudo docker ps -a`.
  - Execute với PostgreSQL `sudo docker compose exec postgresql bash`.
  - Truy vấn cơ sở dữ liệu **Revenue** với `psql -U postgres -d revenue` (Thay đổi thông tin đăng nhập phù hợp).


## Cài đặt và sử dụng Metabase
- Thực hiện `docker compose up --build -d` tạo ra container cho Metabase.
- Truy cập `localhost:3000`, điền các thông tin cá nhân và tạo kết nối tới PostgreSQL:
  - Ip: postgresql. (Do cả 2 PostgreSQL và Metabase đều được triển khai với Docker nên không thể dùng localhost)
  - Host: 5432.
  - Username: postgres.
  - Password: 123456.
  - Database: revenue.


## Khởi chạy Kafka Producer
- Khởi tạo trực tiếp:
  - Truy cập thư mục **KafkaProducer**.
  - Thực hiện `python3 ProducerHN.py`, `python3 ProducerHCM.py` với 2 chi nhánh để đưa dữ liệu lên Kafka.
- Chạy với `Dockerfile`, đóng gói và triển khai với 10 producer chạy song song cho 10 chi nhánh cùng tải dữ liệu lên Kafka.


## Khởi chạy Kafka Consumer
- Truy cập thư  mục **KafkaConsumer** theo công việc, vào file `pom.xml` chỉnh sửa version mới và thực hiện `mvn clean package` để tạo ra file `.jar` mới, file sẽ nằm trong folder `target`.
- Truy cập thư mục lưu trữ **flink** và thực hiện `./bin/flink run -c org.cloud.KafkaConsumerApplication ~/Deploy/KafkaConsumer-v1.0.0.jar` (Thay bằng đường dẫn và phiên bản phù hợp).
- Xem thông tin cấu hình, chi tiết hoạt động các job xử lý dữ liệu trên dashboard của Flink.