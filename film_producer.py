from kafka import KafkaProducer
import pandas as pd
import json
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilmKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='film'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        logger.info(f"Initialized Kafka producer for topic: {topic}")

    def read_csv_data(self, file_path):
        """Read data from CSV file"""
        try:
            # Use semicolon as separator
            df = pd.read_csv(file_path, sep=';')
            logger.info(f"Successfully read {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def process_record(self, record):
        """Process a single record from the CSV"""
        try:
            # Convert record to dictionary and handle NaN values
            data = record.to_dict()
            for key, value in data.items():
                if pd.isna(value):
                    data[key] = None
                elif isinstance(value, float):
                    # Convert float to int if it's a whole number
                    if value.is_integer():
                        data[key] = int(value)
                    else:
                        data[key] = value
                elif isinstance(value, int):
                    # Keep integers as is
                    data[key] = value
                elif isinstance(value, str):
                    data[key] = value.strip()
            
            # Add timestamp
            data['processed_at'] = datetime.now().isoformat()
            
            return data
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return None

    def send_to_kafka(self, data):
        """Send data to Kafka topic"""
        try:
            # Process the record
            processed_data = self.process_record(data)
            if processed_data:
                # Send to Kafka
                self.producer.send(self.topic, processed_data)
                self.producer.flush()
                logger.info(f"Sent record to Kafka: {processed_data.get('show_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")

    def produce_messages(self, file_path, batch_size=100):
        """Produce messages from CSV file to Kafka"""
        try:
            # Read CSV data
            df = self.read_csv_data(file_path)
            
            # Process in batches
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                for _, record in batch.iterrows():
                    self.send_to_kafka(record)
                
                logger.info(f"Processed batch {i//batch_size + 1} of {(len(df)-1)//batch_size + 1}")
            
            logger.info("Finished producing messages")
            
        except Exception as e:
            logger.error(f"Error in produce_messages: {e}")
        finally:
            self.producer.close()

def main():
    # File path
    csv_file_path = 'D:\git_hub_post_dtdm\data\Film.csv'
    
    # Initialize producer
    producer = FilmKafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='film'
    )
    
    # Produce messages
    producer.produce_messages(csv_file_path, batch_size=100)

if __name__ == "__main__":
    main() 