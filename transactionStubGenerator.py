import json
import random
import time
import datetime

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime

# Set up the Faker object
fake = Faker()


def transaction_category_to_descriptions():
    transaction_categories = ['groceries', 'shopping', 'entertainment', 'transport', 'eating_out', 'bills', 'health',
                              'travel', 'cash', 'general', 'income']
    allowed_transaction_description = {'groceries': ['Tesco', 'Sainsbury', 'Asda', 'Morrisons'],
                                       'shopping': ['Amazon', 'eBay', 'Argos', 'John Lewis'],
                                       'entertainment': ['Netflix', 'Spotify', 'Sky', 'Disney+'],
                                       'transport': ['TFL', 'Uber', 'Bolt', 'Addison Lee'],
                                       'eating_out': ['McDonalds', 'KFC', 'Nandos', 'Pizza Hut'],
                                       'bills': ['British Gas', 'Thames Water', 'Sky', 'BT'],
                                       'health': ['Boots', 'Superdrug', 'Holland & Barrett', 'Lloyds Pharmacy'],
                                       'travel': ['British Airways', 'EasyJet', 'Ryanair', 'Virgin Atlantic'],
                                       'cash': ['ATM'],
                                       'income': ['Salary', 'Bonus', 'Interest', 'Dividend', 'Refund',
                                                  'Transfer'],
                                       'general': ['Other']}
    category = random.choice(transaction_categories)
    description = random.choice(allowed_transaction_description[category])
    return {'Category': category, 'Description': description}


def customer_to_account_number():
    customer_id = ['1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010']
    allowed_accounts_per_customer = {'1001': ['6958320548159'],
                                     '1002': ['31633881077550', '31633881077551'],
                                     '1003': ['31633881077553'],
                                     '1004': ['31633881077556', '31633881077557'],
                                     '1005': ['31633881077559'],
                                     '1006': ['31633881077562', '31633881077563'],
                                     '1007': ['31633881077565'],
                                     '1008': ['31633881077568', '31633881077569', '31633881077570'],
                                     '1009': ['31633881077571'],
                                     '1010': ['31633881077574', '31633881077575']}
    customerid = random.choice(customer_id)
    accountnumber = random.choice(allowed_accounts_per_customer[customerid])
    return {'customerId': customerid, 'accountNumber': accountnumber}


# Set up the schema for the transaction
transaction_schema = {
    "type": "record",
    "name": "transaction",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "transaction_time", "type": "datetime"},
        {"name": "transaction_description", "type": "string"},
        {"name": "transaction_amount", "type": "float"},
        {"name": "transaction_type", "type": "string"},
        {"name": "transaction_currency", "type": "string"},
        {"name": "transaction_category", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "merchant", "type": "string"},
        {"name": "customerId", "type": "string"},
        {"name": "accountNumber", "type": "string"}
    ],
}

# Set up the Kafka Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = SerializingProducer(producer_conf)


def generate_transaction():
    # Generate the transaction data
    customerAccount = customer_to_account_number()
    categoryDescription = transaction_category_to_descriptions()
    transaction_id = fake.uuid4()
    transaction_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    transaction_amount = round(random.uniform(1.0, 1000.0), 2)
    transaction_category = categoryDescription["Category"]
    transaction_description = categoryDescription["Description"]
    transaction_type = 'credit' if transaction_category == 'income' else 'debit'
    transaction_currency = random.choice(['GBP'])
    location = fake.city()
    merchant = 'income' if transaction_category == 'income' else fake.company()
    customerid = customerAccount["customerId"]
    accountnumber = customerAccount["accountNumber"]

    # Create the transaction record
    transaction = {
        "transaction_id": transaction_id,
        "transaction_time": transaction_time,
        "transaction_category": transaction_category,
        "transaction_description": transaction_description,
        "transaction_amount": transaction_amount,
        "transaction_type": transaction_type,
        "transaction_currency": transaction_currency,
        "location": location,
        "merchant": merchant,
        "customerId": customerid,
        "accountNumber": accountnumber
    }

    return transaction


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Transaction record {}: {}".format(msg.key(), err))
        return
    print('Transaction record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    # Set up the topic
    topic = 'transactions'

    # Set up the start time
    curr_time = datetime.now()

    # Generate the transactions
    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_transaction()
            print(json.dumps(transaction, indent=2))
            producer.produce(topic=topic, key=transaction['transaction_id'], value=transaction, on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(random.uniform(0.1, 1.0))
        except BufferError as e:
            print('Local producer queue is full ... backing off for 1 second')
            time.sleep(1.0)
        except Exception as e:
            print('Failed to generate transaction: {}'.format(e))
        except KeyboardInterrupt:
            break
    producer.flush()


if __name__ == '__main__':
    main()
