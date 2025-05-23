import json
import time
import ProducerConnection as connection
from SendHelper import get_timestamp

# Create Producer Kafka
producer = connection.getConnection()
topicName = "InvoiceTopic"
count = 0
storeName = 'HN'

# Load data from JSON file
with open("data/transactions_data_HN.json", encoding="utf-8") as jsonfile:
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
                "Store": storeName
            }

            # Send message to kafka
            producer.send(topicName, data)
            count += 1
            print(f"Sent item {item['ProductID']} of transaction \"{transaction_id}\" to Kafka Topic \"{topicName}\"")

    print(f"{storeName} Store: {count} items had sent successfully!")

    executionTime = time.time() - startTime
    print(f"Total time: {executionTime:.2f} seconds")

connection.closeConnection(producer)
