import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

class UserInteractionsProducerAws:
    def __init__(self, user_profiles_path, product_data_path, kafka_bootstrap_servers, topic_name, num_days=30):
        self.user_profiles = pd.read_csv(user_profiles_path)
        self.product_data = pd.read_csv(product_data_path)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic_name = topic_name
        self.num_days = num_days
        self.start_time = datetime(2023, 11, 1, 0, 0, 0)
        self.interaction_id_counter = 1
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=self.serialize).encode('utf-8')
        )

    def serialize(self, obj):
        if isinstance(obj, np.int64):
            return int(obj)
        raise TypeError("Type not serializable")

    def generate_interaction_data(self):
        for day in range(self.num_days):
            for minute in range(24 * 60):  # 24 hours * 60 minutes
                timestamp = self.start_time + timedelta(days=day, minutes=minute)
                for user_id in self.user_profiles['User_ID'].tolist():
                    interaction_type = np.random.choice(['product_search', 'product_view', 'add_to_cart', 'purchase'])
                    product_id, additional_attributes = self.generate_interaction_details(interaction_type)

                    interaction_data = {
                        'Interaction_ID': self.interaction_id_counter,
                        'Timestamp': str(timestamp),
                        'User_ID': int(user_id),
                        'Interaction_Type': interaction_type,
                        'Product_ID': product_id,
                        **additional_attributes
                    }

                    self.interaction_id_counter += 1
                    self.publish_to_kafka(interaction_data)

    def generate_interaction_details(self, interaction_type):
        if interaction_type == 'product_search':
            search_query = np.random.choice(['laptop', 'phone', 'clothing', 'shoes', 'electronics'])
            return ('-', {'Search_Query': search_query})
        elif interaction_type == 'product_view':
            product_id = np.random.choice(self.product_data['Product_ID'].tolist())
            product_name = self.product_data.loc[self.product_data['Product_ID'] == product_id, 'Product_Name'].values[0]
            return (int(product_id), {'Product_Name': product_name})
        elif interaction_type == 'add_to_cart':
            product_id = np.random.choice(self.product_data['Product_ID'].tolist())
            product_name = self.product_data.loc[self.product_data['Product_ID'] == product_id, 'Product_Name'].values[0]
            quantity = np.random.randint(1, 5)
            return (int(product_id), {'Product_Name': product_name, 'Quantity': int(quantity)})
        elif interaction_type == 'purchase':
            product_id = np.random.choice(self.product_data['Product_ID'].tolist())
            product_name = self.product_data.loc[self.product_data['Product_ID'] == product_id, 'Product_Name'].values[0]
            quantity = np.random.randint(1, 5)
            payment_method = np.random.choice(['Credit Card', 'Debit Card', 'PayPal'])
            return (int(product_id), {'Product_Name': product_name, 'Quantity': int(quantity), 'Payment_Method': payment_method})

    def publish_to_kafka(self, data):
        try:
            self.producer.send(self.topic_name, value=data)
        except Exception as e:
            print(f"Error publishing data to Kafka: {e}")

    def close_producer(self):
        self.producer.close()

# Local file paths on EMR cluster
user_profiles_path = '/home/hadoop/UserProfiles_data.csv'
product_data_path = '/home/hadoop/ProductData_data.csv'

# Kafka bootstrap servers and topic
bootstrap_servers = 'localhost:9092'
topic_name = 'User_Interactions'

# Example usage
producer = UserInteractionsProducerAws(user_profiles_path, product_data_path, bootstrap_servers, topic_name, num_days=30)
producer.generate_interaction_data()
producer.close_producer()

