from kafka import KafkaConsumer
import json
import logging
from datetime import datetime
import pandas as pd
from collections import defaultdict
import os
import shutil
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TicketKafkaConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='ticket', read_from_beginning=False):
        # Tạo group_id ngẫu nhiên nếu muốn đọc từ đầu
        group_id = None if read_from_beginning else 'ticket-consumer-group'
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,  # None sẽ tạo group_id ngẫu nhiên mỗi lần chạy
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic = topic
        
        # Set up output directory
        self.output_dir = 'processed_ticket_data'
        self.cleanup_old_data()
            
        logger.info(f"Initialized Kafka consumer for topic: {topic}")
        if read_from_beginning:
            logger.info("Consumer will read all messages from the beginning")
        else:
            logger.info("Consumer will read only new messages or continue from last position")

    def cleanup_old_data(self):
        try:
            # Remove the directory if it exists
            if os.path.exists(self.output_dir):
                shutil.rmtree(self.output_dir)
                logger.info(f"Removed old data directory: {self.output_dir}")
            
            # Create a fresh directory
            os.makedirs(self.output_dir)
            logger.info(f"Created new data directory: {self.output_dir}")
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            raise

    def process_ticket(self, ticket):
        try:
            # Extract and format the data
            metrics = {
                'orderid': ticket['orderid'],
                'cashier': ticket['cashier'],
                'saledate': ticket['saledate'],
                'total': ticket['total'],
                'customerid': ticket['customerid'],
                'ticketcode': ticket['ticketcode'],
                'date': ticket['date'],
                'time': ticket['time'],
                'slot': ticket['slot'],
                'room': ticket['room'],
                'film': ticket['film'],
                'slot_type': ticket['slot_type'],
                'ticket_type': ticket['ticket_type'],
                'ticket_price': ticket['ticket_price'],
                'popcorn': ticket['popcorn'],
                'processed_at': ticket.get('processed_at', datetime.now().isoformat())
            }
            
            # Log the processed ticket
            logger.info(f"Processed ticket: {metrics['orderid']}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error processing ticket: {e}")
            return None

    def save_to_csv(self, data, filename):
        try:
            df = pd.DataFrame(data)
            filepath = os.path.join(self.output_dir, filename)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved data to {filepath}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def analyze_tickets(self, tickets):
        try:
            df = pd.DataFrame(tickets)
            
            # Chuyển đổi các cột số
            df['total'] = pd.to_numeric(df['total'], errors='coerce')
            df['ticket_price'] = pd.to_numeric(df['ticket_price'], errors='coerce')
            
            # Chuyển đổi cột thời gian
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df['hour'] = df['datetime'].dt.hour
            df['day'] = df['datetime'].dt.date
            df['month'] = df['datetime'].dt.month
            
            # Phân tích cơ bản
            stats = {
                'total_tickets': len(df),
                'total_revenue': df['total'].sum(),
                'avg_ticket_price': df['ticket_price'].mean(),
                'film_distribution': df['film'].value_counts().to_dict(),
                'ticket_type_distribution': df.groupby('ticket_type')['customerid'].nunique().to_dict(),
                'room_distribution': df['room'].value_counts().to_dict(),
                'slot_type_distribution': df['slot_type'].value_counts().to_dict(),
                'popcorn_distribution': df['popcorn'].value_counts().to_dict(),
                'sales_by_cashier': df.groupby('cashier')['total'].agg(['count', 'sum']).to_dict(),
                
                # Thêm phân tích theo thời gian
                'hourly_ticket_sales': df.groupby('hour')['orderid'].count().to_dict(),
                'hourly_revenue': df.groupby('hour')['total'].sum().to_dict(),
                'daily_revenue': df.groupby('day')['total'].sum().to_dict(),
                'monthly_revenue': df.groupby('month')['total'].sum().to_dict(),
                'daily_ticket_sales': df.groupby('day')['orderid'].count().to_dict(),
                'monthly_ticket_sales': df.groupby('month')['orderid'].count().to_dict()
            }
            
            # Phân tích chi tiết theo phim
            film_analysis = df.groupby('film').agg({
                'orderid': 'count',  # Số lượng vé
                'customerid': 'nunique',  # Số lượng thành viên
                'total': 'sum',      # Doanh thu
                'ticket_price': 'mean',  # Giá vé trung bình
                'slot_type': lambda x: x.value_counts().to_dict()  # Phân bố loại ghế
            }).reset_index()
            
            # Tính toán lợi nhuận và thêm vào film_analysis
            film_analysis['revenue_per_ticket'] = film_analysis['total'] / film_analysis['orderid']
            film_analysis['tickets_per_customer'] = film_analysis['orderid'] / film_analysis['customerid']
            
            # Lưu phân tích phim vào file
            film_analysis_dict = []
            for _, row in film_analysis.iterrows():
                film_data = {
                    'Film': row['film'],
                    'Total_Tickets': row['orderid'],
                    'Total_Customers': row['customerid'],
                    'Tickets_Per_Customer': row['tickets_per_customer'],
                    'Total_Revenue': row['total'],
                    'Avg_Ticket_Price': row['ticket_price'],
                    'Revenue_Per_Ticket': row['revenue_per_ticket'],
                    'Seat_Distribution': row['slot_type']
                }
                film_analysis_dict.append(film_data)
            
            self.save_to_csv(film_analysis_dict, 'film_detailed_analysis.csv')
            
            # Phân tích loại ghế
            seat_analysis = df.groupby(['film', 'slot_type']).agg({
                'orderid': 'count',
                'customerid': 'nunique',  # Thêm số lượng thành viên
                'total': 'sum'
            }).reset_index()
            
            self.save_to_csv(seat_analysis.to_dict('records'), 'seat_type_analysis.csv')
            
            # Save summary results
            self.save_to_csv([{
                'metric': 'Total Tickets',
                'value': stats['total_tickets']
            }, {
                'metric': 'Total Unique Customers',
                'value': df['customerid'].nunique()
            }, {
                'metric': 'Average Tickets per Customer',
                'value': stats['total_tickets'] / df['customerid'].nunique()
            }, {
                'metric': 'Total Revenue',
                'value': stats['total_revenue']
            }, {
                'metric': 'Average Ticket Price',
                'value': stats['avg_ticket_price']
            }], 'ticket_summary.csv')
            
            # Save detailed distributions
            self.save_to_csv(pd.DataFrame(stats['film_distribution'].items(), 
                           columns=['Film', 'Count']), 'film_distribution.csv')
            
            # Lưu phân bố loại vé theo số lượng thành viên
            ticket_type_df = pd.DataFrame(stats['ticket_type_distribution'].items(), 
                           columns=['Ticket Type', 'Unique Customers'])
            self.save_to_csv(ticket_type_df, 'ticket_type_distribution.csv')
            
            # Lưu phân tích theo thời gian
            # Phân tích theo giờ
            hourly_analysis = pd.DataFrame({
                'Hour': list(stats['hourly_ticket_sales'].keys()),
                'Ticket_Sales': list(stats['hourly_ticket_sales'].values()),
                'Revenue': list(stats['hourly_revenue'].values())
            })
            self.save_to_csv(hourly_analysis.to_dict('records'), 'hourly_analysis.csv')
            
            # Phân tích theo ngày
            daily_analysis = pd.DataFrame({
                'Date': list(stats['daily_revenue'].keys()),
                'Ticket_Sales': list(stats['daily_ticket_sales'].values()),
                'Revenue': list(stats['daily_revenue'].values())
            })
            self.save_to_csv(daily_analysis.to_dict('records'), 'daily_analysis.csv')
            
            # Phân tích theo tháng
            monthly_analysis = pd.DataFrame({
                'Month': list(stats['monthly_revenue'].keys()),
                'Ticket_Sales': list(stats['monthly_ticket_sales'].values()),
                'Revenue': list(stats['monthly_revenue'].values())
            })
            self.save_to_csv(monthly_analysis.to_dict('records'), 'monthly_analysis.csv')
            
            # Log chi tiết kết quả phân tích
            logger.info("\nTicket Analysis Results:")
            logger.info(f"Total Tickets: {stats['total_tickets']}")
            logger.info(f"Total Unique Customers: {df['customerid'].nunique()}")
            logger.info(f"Average Tickets per Customer: {stats['total_tickets'] / df['customerid'].nunique():.2f}")
            logger.info(f"Total Revenue: {stats['total_revenue']:,.2f}")
            logger.info(f"Average Ticket Price: {stats['avg_ticket_price']:,.2f}")
            
            logger.info("\nPhim Analysis:")
            for film_data in film_analysis_dict:
                logger.info(f"\nPhim: {film_data['Film']}")
                logger.info(f"- Số vé bán ra: {film_data['Total_Tickets']}")
                logger.info(f"- Số thành viên: {film_data['Total_Customers']}")
                logger.info(f"- Số vé/thành viên: {film_data['Tickets_Per_Customer']:.2f}")
                logger.info(f"- Doanh thu: {film_data['Total_Revenue']:,.2f}")
                logger.info(f"- Giá vé trung bình: {film_data['Avg_Ticket_Price']:,.2f}")
                logger.info(f"- Doanh thu/vé: {film_data['Revenue_Per_Ticket']:,.2f}")
                logger.info("- Phân bố loại ghế:")
                for seat_type, count in film_data['Seat_Distribution'].items():
                    logger.info(f"  + {seat_type}: {count} vé")
            
            # Log thêm thông tin phân tích theo thời gian
            logger.info("\nTime-based Analysis:")
            
            # Phân tích theo giờ
            logger.info("\nHourly Analysis:")
            peak_hour = max(stats['hourly_ticket_sales'].items(), key=lambda x: x[1])
            logger.info(f"Peak hour for ticket sales: {peak_hour[0]}:00 with {peak_hour[1]} tickets")
            
            peak_revenue_hour = max(stats['hourly_revenue'].items(), key=lambda x: x[1])
            logger.info(f"Peak hour for revenue: {peak_revenue_hour[0]}:00 with {peak_revenue_hour[1]:,.2f} revenue")
            
            # Phân tích theo ngày
            logger.info("\nDaily Analysis:")
            peak_day = max(stats['daily_revenue'].items(), key=lambda x: x[1])
            logger.info(f"Highest revenue day: {peak_day[0]} with {peak_day[1]:,.2f} revenue")
            
            # Phân tích theo tháng
            logger.info("\nMonthly Analysis:")
            peak_month = max(stats['monthly_revenue'].items(), key=lambda x: x[1])
            logger.info(f"Highest revenue month: {peak_month[0]} with {peak_month[1]:,.2f} revenue")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error in ticket analysis: {e}")
            logger.error("Error details:", exc_info=True)
            return None

    def start_consuming(self, batch_size=50):
        logger.info("Starting to consume ticket messages...")
        all_tickets = []  # Lưu tất cả vé đã xử lý
        message_count = 0
        start_time = datetime.now()
        
        try:
            logger.info("Bắt đầu đọc dữ liệu từ Kafka...")
            for message in self.consumer:
                # Process the message
                ticket = message.value
                processed_ticket = self.process_ticket(ticket)
                
                if processed_ticket:
                    all_tickets.append(processed_ticket)
                    message_count += 1
                    
                    # Log tiến độ xử lý
                    if message_count % 100 == 0:
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        logger.info(f"Đã xử lý {message_count} vé... (Thời gian: {elapsed_time:.1f}s)")
                        
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            # Lưu toàn bộ dữ liệu vào một file
            if all_tickets:
                logger.info(f"Đang lưu {len(all_tickets)} vé vào file processed_tickets.csv...")
                self.save_to_csv(all_tickets, 'processed_tickets.csv')
            
            # Phân tích toàn bộ dữ liệu đã thu thập
            if all_tickets:
                total_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"\nĐã xử lý tổng cộng {len(all_tickets)} vé trong {total_time:.1f} giây")
                logger.info(f"Tốc độ xử lý: {len(all_tickets)/total_time:.1f} vé/giây")
                logger.info("Bắt đầu phân tích toàn bộ dữ liệu...")
                self.analyze_tickets(all_tickets)
            else:
                logger.info("Không có dữ liệu để phân tích")
            
            self.consumer.close()

def main():
    # Thêm argument để chọn cách đọc messages
    parser = argparse.ArgumentParser()
    parser.add_argument('--read-all', action='store_true', 
                      help='Read all messages from beginning')
    args = parser.parse_args()

    # Initialize the consumer
    consumer = TicketKafkaConsumer(
        bootstrap_servers='localhost:9092',
        topic='ticket',
        read_from_beginning=args.read_all
    )
    
    # Start consuming messages
    consumer.start_consuming(batch_size=50)

if __name__ == "__main__":
    main() 