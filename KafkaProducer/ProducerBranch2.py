import json
import time
import ProducerConnection as connection

def run():
    producer = connection.getConnection()
    topicName = "branch-2"
    count = 0

    with open("data/transactions_data_branch_2.json", encoding="utf-8") as jsonfile:
        transactions = json.load(jsonfile)
        startTime = time.time()

        for transaction in transactions:
            transaction_id = transaction["TransactionID"]
            customer_id = transaction["CustomerID"]
            timestamp = transaction["Timestamp"]
            items = transaction["Items"]

            for item in items:
                data = {
                    "TransactionID": transaction_id,
                    "CustomerID": customer_id,
                    "TransactionTime": timestamp,
                    "ProductID": item["ProductID"],
                    "Quantity": item["Quantity"],
                    "TotalPrice": item["Price"] * item["Quantity"],
                    "Branch": "2"
                }

                producer.send(topicName, value=data)
                producer.flush()
                count += 1
                print(f"[Branch 2] ➜ Sent item {item['ProductID']} from transaction {transaction_id}")

        executionTime = time.time() - startTime
        print(f"[Branch 2] ✅ {count} items sent in {executionTime:.2f} seconds")

    connection.closeConnection(producer)
