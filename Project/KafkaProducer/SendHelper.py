# SendHelper.py
import json
import time
import os
from datetime import datetime

def extract_branch_id(file_path):
    base = os.path.basename(file_path)
    parts = base.replace(".json", "").split("_")
    return parts[-1]

def build_message(transaction, item, branch_id):
    return {
        "TransactionID": transaction["TransactionID"],
        "CustomerID": transaction["CustomerID"],
        "TransactionTime": datetime.utcnow().isoformat(),  # Thời gian gửi
        "ProductID": item["ProductID"],
        "Quantity": item["Quantity"],
        "TotalPrice": item["Price"] * item["Quantity"],
        "Branch": branch_id
    }

def send_data_kafka(producer, file_path, branch_id=None, topic_name="InvoiceTopic", delay=0.1, print_log=True, send_done=True):
    count = 0
    start_time = time.time()

    if branch_id is None:
        branch_id = extract_branch_id(file_path)

    with open(file_path, encoding="utf-8") as jsonfile:
        transactions = json.load(jsonfile)

        for transaction in transactions:
            for item in transaction["Items"]:
                data = build_message(transaction, item, branch_id)
                producer.send(topic_name, data, key=branch_id.encode('utf-8'))
                time.sleep(delay)

                count += 1
                if print_log:
                    print(f"[Branch {branch_id}] Sent item {data['ProductID']} of transaction \"{data['TransactionID']}\" to topic \"{topic_name}\"")

    duration = time.time() - start_time
    if print_log:
        print(f"[Branch {branch_id}] Sent {count} items in {duration:.2f} seconds.")