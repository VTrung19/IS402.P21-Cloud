import ProducerConnection as connection
from SendHelper import send_data_kafka

def run():
    producer = connection.getConnection()
    file_path = "data/transactions_data_branch_5.json"

    # Gọi hàm chung với các tùy chọn
    send_data_kafka(
        producer=producer,
        file_path=file_path,
        delay=10,
        print_log=True,
        send_done=True
    )

    connection.closeConnection(producer)
