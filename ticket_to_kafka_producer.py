from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import pandas as pd
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TicketKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='ticket'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Kiểm tra kết nối Kafka
        try:
            logger.info(f"Attempting to connect to Kafka at {bootstrap_servers}")
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topics = admin_client.list_topics()
            logger.info(f"Connected to Kafka. Available topics: {topics}")
            
            # Kiểm tra topic tồn tại, nếu không thì tạo mới
            if topic not in topics:
                logger.info(f"Topic {topic} does not exist. Creating new topic...")
                admin_client.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
                logger.info(f"Created topic {topic}")
            
            admin_client.close()
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}")
            raise

        # Khởi tạo producer với cấu hình đơn giản hơn
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,
                retries=3
            )
            logger.info(f"Successfully initialized Kafka producer for topic: {topic}")
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            raise

    def read_csv(self, file_path):
        try:
            logger.info(f"Reading CSV file: {file_path}")
            df = pd.read_csv(file_path, sep=';')
            logger.info(f"Successfully read CSV file. Total records: {len(df)}")
            logger.debug(f"First few records:\n{df.head()}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def format_ticket(self, ticket):
        try:
            # Kiểm tra và chuyển đổi từng trường
            # Giữ nguyên orderid và customerid dạng string vì chúng có thể chứa ký tự
            orderid = str(ticket['orderid'])
            cashier = str(ticket['cashier'])
            saledate = pd.to_datetime(ticket['saledate']).strftime('%Y-%m-%d %H:%M:%S')
            total = float(ticket['total'])
            customerid = str(ticket['customerid'])
            ticketcode = str(ticket['ticketcode'])
            date = pd.to_datetime(ticket['date']).strftime('%Y-%m-%d')
            time = str(ticket['time'])
            slot = str(ticket['slot'])
            room = str(ticket['room'])
            film = str(ticket['film'])
            slot_type = str(ticket['slot type'])
            ticket_type = str(ticket['ticket type'])
            ticket_price = float(ticket['ticket price'])
            popcorn = str(ticket['popcorn'])
            
            formatted = {
                "orderid": orderid,
                "cashier": cashier,
                "saledate": saledate,
                "total": total,
                "customerid": customerid,
                "ticketcode": ticketcode,
                "date": date,
                "time": time,
                "slot": slot,
                "room": room,
                "film": film,
                "slot_type": slot_type,
                "ticket_type": ticket_type,
                "ticket_price": ticket_price,
                "popcorn": popcorn,
                "processed_at": datetime.now().isoformat()
            }
            logger.debug(f"Formatted ticket: {formatted}")
            return formatted
        except Exception as e:
            logger.error(f"Error formatting ticket: {e}")
            logger.error(f"Problematic ticket data: {ticket}")
            return None

    def send_ticket(self, ticket):
        try:
            formatted = self.format_ticket(ticket)
            if formatted:
                future = self.producer.send(self.topic, value=formatted)
                record_metadata = future.get(timeout=10)
                logger.debug(f"Message sent to topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error sending ticket: {e}")
            return False

    def process_tickets(self, file_path, batch_size=100):
        try:
            df = self.read_csv(file_path)
            total_tickets = len(df)
            processed_count = 0
            start_time = time.time()
            
            logger.info(f"Starting to process {total_tickets} tickets")
            
            # Xử lý từng batch
            for i in range(0, total_tickets, batch_size):
                batch = df.iloc[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} of {(total_tickets-1)//batch_size + 1}")
                
                for _, ticket in batch.iterrows():
                    if self.send_ticket(ticket):
                        processed_count += 1
                        if processed_count % 100 == 0:
                            elapsed_time = time.time() - start_time
                            rate = processed_count / elapsed_time if elapsed_time > 0 else 0
                            logger.info(f"Processed {processed_count}/{total_tickets} tickets. Rate: {rate:.2f} messages/second")
                
                # Flush sau mỗi batch
                self.producer.flush()
                logger.info(f"Flushed batch {i//batch_size + 1}")
            
            # Log kết quả cuối cùng
            total_time = time.time() - start_time
            final_rate = processed_count / total_time if total_time > 0 else 0
            logger.info(f"\nProcessing completed:")
            logger.info(f"Total tickets processed: {processed_count}/{total_tickets}")
            logger.info(f"Total time: {total_time:.2f} seconds")
            logger.info(f"Average rate: {final_rate:.2f} messages/second")
                
        except Exception as e:
            logger.error(f"Error processing tickets: {e}")
            logger.error("Error details:", exc_info=True)
        finally:
            try:
                self.producer.flush(timeout=5)
                logger.info("Flushed all remaining messages")
                self.producer.close(timeout=5)
                logger.info("Closed producer connection")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")

def main():
    try:
        producer = TicketKafkaProducer(
            bootstrap_servers='localhost:9092',
            topic='ticket'
        )
        
        csv_file_path = 'D:\git_hub_post_dtdm\data\Ticket.csv'
        producer.process_tickets(
            file_path=csv_file_path,
            batch_size=100  # Giảm batch size để dễ debug
        )
    except Exception as e:
        logger.error(f"Main process error: {e}")
        logger.error("Error details:", exc_info=True)

if __name__ == "__main__":
    main() 