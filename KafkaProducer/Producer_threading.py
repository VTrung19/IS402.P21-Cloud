import threading
import ProducerBranch1
import ProducerBranch2
import ProducerBranch3
import ProducerBranch4
import ProducerBranch5
import ProducerBranch6
import ProducerBranch7
import ProducerBranch8
import ProducerBranch9
import ProducerBranch10

# Tạo các luồng gửi dữ liệu cho từng chi nhánh
threads = [
    threading.Thread(target=ProducerBranch1.run),
    threading.Thread(target=ProducerBranch2.run),
    threading.Thread(target=ProducerBranch3.run),
    threading.Thread(target=ProducerBranch4.run),
    threading.Thread(target=ProducerBranch5.run),
    threading.Thread(target=ProducerBranch6.run),
    threading.Thread(target=ProducerBranch7.run),
    threading.Thread(target=ProducerBranch8.run),
    threading.Thread(target=ProducerBranch9.run),
    threading.Thread(target=ProducerBranch10.run)
]

# Bắt đầu các luồng
for t in threads:
    t.start()

# Chờ tất cả các luồng kết thúc
for t in threads:
    t.join()

print("Đã gửi dữ liệu từ tất cả các chi nhánh.")
