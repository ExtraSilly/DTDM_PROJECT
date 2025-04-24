from kafka import KafkaProducer
import pandas as pd
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TicketKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='ticket'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.topic = topic
        logger.info(f"Initialized Kafka producer for topic: {topic}")

    def read_csv(self, file_path):
        try:
            df = pd.read_csv(file_path, sep=';')  # Sử dụng dấu chấm phẩy làm separator
            logger.info(f"Successfully read CSV file: {file_path}")
            logger.info(f"Total records found: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def send_ticket(self, ticket):
        try:
            # Format dữ liệu theo cấu trúc mong muốn
            data = {
                "orderid": ticket['orderid'],
                "cashier": ticket['cashier'],
                "saledate": pd.to_datetime(ticket['saledate']).strftime('%Y-%m-%d %H:%M:%S'),
                "total": ticket['total'],
                "customerid": ticket['customerid'],
                "ticketcode": str(ticket['ticketcode']),
                "date": pd.to_datetime(ticket['date']).strftime('%Y-%m-%d'),
                "time": ticket['time'],
                "slot": ticket['slot'],
                "room": str(ticket['room']),
                "film": ticket['film'],
                "slot_type": ticket['slot type'],
                "ticket_type": ticket['ticket type'],
                "ticket_price": ticket['ticket price'],
                "popcorn": ticket['popcorn'],
                "processed_at": datetime.now().isoformat()
            }

            # Gửi dữ liệu đến Kafka
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)
            
            # Log thông tin chi tiết
            logger.info(f"Successfully sent Ticket - Order ID: {data['orderid']}")
            logger.info(f"Data sent: {json.dumps(data, indent=2, ensure_ascii=False)}")
            
        except Exception as e:
            logger.error(f"Error sending ticket: {e}")
            logger.error(f"Problematic ticket data: {ticket}")

    def process_tickets(self, file_path, batch_size=50, delay=0.1):
        try:
            df = self.read_csv(file_path)
            total_tickets = len(df)
            logger.info(f"Starting to process {total_tickets} tickets")
            
            for i in range(0, total_tickets, batch_size):
                batch = df.iloc[i:i+batch_size]
                for _, ticket in batch.iterrows():
                    self.send_ticket(ticket)
                    time.sleep(delay)
                
                logger.info(f"Processed {min(i+batch_size, total_tickets)}/{total_tickets} tickets")
                
        except Exception as e:
            logger.error(f"Error processing tickets: {e}")
            logger.error("Error details:", exc_info=True)
        finally:
            self.producer.close()

def main():
    # Initialize the producer
    producer = TicketKafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='ticket'
    )
    
    # Process the CSV file
    csv_file_path = 'D:/Connect_Pub_Sub_DTDM/data/Ticket.csv'
    producer.process_tickets(csv_file_path, batch_size=50, delay=0.1)

if __name__ == "__main__":
    main() 