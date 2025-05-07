from kafka import KafkaProducer
import pandas as pd
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='customer'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,  # Giảm độ tin cậy để tăng tốc độ
            retries=2,  # Giảm số lần retry
            # Tăng kích thước batch
            batch_size=32768,  # 32KB
            # Giảm thời gian chờ
            linger_ms=50,  # 50ms
            # Tăng buffer size
            buffer_memory=67108864,  # 64MB
            # Tăng kích thước message tối đa
            max_request_size=2097152,  # 2MB
            # Tăng số lượng message trong một batch
            max_in_flight_requests_per_connection=10,
            # Giảm thời gian timeout
            request_timeout_ms=15000,  # 15 seconds
            # Giảm thời gian chờ retry
            retry_backoff_ms=500,  # 0.5 second
            # Bật compression
            compression_type='gzip'
        )
        self.topic = topic
        logger.info(f"Initialized Kafka producer for topic: {topic}")

    def read_csv(self, file_path):
        """Read CSV file and return DataFrame"""
        try:
            # Note: Using semicolon as separator based on the CSV format
            df = pd.read_csv(file_path, sep=';')
            logger.info(f"Successfully read CSV file: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def send_customer(self, customer):
        """Send a single customer to Kafka"""
        try:
            # Convert customer to dictionary
            data = customer.to_dict()
            
            # Add processing timestamp
            data['processed_at'] = datetime.now().isoformat()
            
            # Clean the data if needed
            data['customerid'] = str(data['customerid'])
            
            # Gửi không đợi response
            self.producer.send(self.topic, value=data)
            
            # Chỉ log mỗi 1000 records
            if int(data['customerid']) % 1000 == 0:
                logger.info(f"Sent customer: {data['customerid']}")
                
        except Exception as e:
            logger.error(f"Error sending customer: {e}")

    def process_customers(self, file_path, batch_size=500, delay=0.01):
        """Process CSV file and send customers to Kafka"""
        try:
            df = self.read_csv(file_path)
            total_customers = len(df)
            logger.info(f"Starting to process {total_customers} customers")
            
            for i in range(0, total_customers, batch_size):
                batch = df.iloc[i:i+batch_size]
                for _, customer in batch.iterrows():
                    self.send_customer(customer)
                    time.sleep(delay)  # Giảm delay xuống 0.01s
                
                logger.info(f"Processed {min(i+batch_size, total_customers)}/{total_customers} customers")
                
        except Exception as e:
            logger.error(f"Error processing customers: {e}")
        finally:
            # Đợi tất cả message được gửi đi
            self.producer.flush()
            self.producer.close()

def main():
    # Initialize the producer
    producer = CustomerKafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='customer'
    )
    
    # Process the CSV file
    csv_file_path = 'D:\git_hub_post_dtdm\data\Customer.csv'
    producer.process_customers(csv_file_path, batch_size=500, delay=0.01)

if __name__ == "__main__":
    main() 