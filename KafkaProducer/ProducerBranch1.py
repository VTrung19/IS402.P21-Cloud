import json
import time
import ProducerConnection as connnection

def run():
    # Create Producer Kafka
    producer = connnection.getConnection()
    topicName = "branch-1"
    count = 0

    # Load data from JSON file
    with open("data/transactions_data_branch_1.json", encoding="utf-8") as jsonfile:
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
                    "Branch": "1"
                }

                # Send message to kafka
                producer.send(topicName, data)
                producer.flush()
                count += 1
                print(f"Sent item {item['ProductID']} of transaction \"{transaction_id}\" to Kafka Topic \"{topicName}\"")

        # Send done message
        #producer.send(topicName, {"status": "Done"})
        #producer.flush()
        print(f"Branch 1: {count} items had sent successfully!")

        executionTime = time.time() - startTime
        print(f"Total time: {executionTime:.2f} seconds")

    connnection.closeConnection(producer)
