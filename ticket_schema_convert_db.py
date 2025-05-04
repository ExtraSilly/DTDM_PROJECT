import pandas as pd
import json
import logging
import os
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_schema_converter.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DBSchemaConverter:
    def __init__(self, input_dir='processed_ticket_data', output_dir='db_schema_ticket_data'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.setup_directories()
        
    def setup_directories(self):
        try:
            # Create output directory if it doesn't exist
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                logger.info(f"Created output directory: {self.output_dir}")
            else:
                # Xóa tất cả các file CSV cũ trong thư mục output
                self.cleanup_old_files()
            
            # Verify input directory exists
            if not os.path.exists(self.input_dir):
                raise FileNotFoundError(f"Input directory not found: {self.input_dir}")
                
        except Exception as e:
            logger.error(f"Error setting up directories: {e}")
            raise
            
    def cleanup_old_files(self):
        try:
            # Danh sách các file cần xóa
            files_to_delete = [
                'tickets.csv', 'sales.csv', 'films.csv', 'rooms.csv',
                'film_room.csv', 'ticket_types.csv', 'seat_types.csv',
                'schema_documentation.txt'
            ]
            
            # Xóa từng file
            for filename in files_to_delete:
                filepath = os.path.join(self.output_dir, filename)
                if os.path.exists(filepath):
                    os.remove(filepath)
                    logger.info(f"Deleted old file: {filepath}")
            
            logger.info("Cleanup of old files completed")
        except Exception as e:
            logger.error(f"Error cleaning up old files: {e}")
            logger.error("Error details:", exc_info=True)

    def read_processed_data(self):
        try:
            filepath = os.path.join(self.input_dir, 'processed_tickets.csv')
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Processed data file not found: {filepath}")
            
            logger.info(f"Reading data from {filepath}")
            
            # Đọc dữ liệu với các tùy chọn xử lý lỗi
            df = pd.read_csv(filepath, na_values=['', 'NA', 'null', 'NULL'], keep_default_na=True)
            
            # Kiểm tra và hiển thị thông tin về dữ liệu
            logger.info(f"Successfully read processed data: {len(df)} records")
            logger.info(f"Columns in data: {', '.join(df.columns)}")
            
            # Kiểm tra dữ liệu trống
            null_counts = df.isnull().sum()
            if null_counts.any():
                logger.warning(f"Found null values in columns: {null_counts[null_counts > 0].to_dict()}")
            
            # Hiển thị một số mẫu dữ liệu
            logger.info("Sample data (first 5 rows):")
            logger.info(df.head().to_string())
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading processed data: {e}")
            logger.error("Error details:", exc_info=True)
            raise

    def convert_to_db_schema(self, df):
        try:
            # Kiểm tra các cột cần thiết
            required_columns = [
                'orderid', 'customerid', 'ticketcode', 'date', 'time',
                'slot', 'room', 'film', 'slot_type', 'ticket_type',
                'ticket_price', 'popcorn', 'processed_at', 'cashier', 'saledate', 'total'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing columns in data: {missing_columns}")
                # Thêm các cột còn thiếu với giá trị mặc định
                for col in missing_columns:
                    df[col] = None
                    logger.info(f"Added missing column: {col}")
            
            # Tickets table
            tickets_df = df[[
                'orderid', 'customerid', 'ticketcode', 'date', 'time',
                'slot', 'room', 'film', 'slot_type', 'ticket_type',
                'ticket_price', 'popcorn', 'processed_at'
            ]].copy()
            
            # Đảm bảo không có giá trị null trong các trường quan trọng
            tickets_df['orderid'] = tickets_df['orderid'].fillna('UNKNOWN')
            tickets_df['customerid'] = tickets_df['customerid'].fillna('UNKNOWN')
            tickets_df['film'] = tickets_df['film'].fillna('UNKNOWN')
            
            # Sales table
            sales_df = df[[
                'orderid', 'cashier', 'saledate', 'total'
            ]].copy()
            
            # Đảm bảo không có giá trị null trong các trường quan trọng
            sales_df['orderid'] = sales_df['orderid'].fillna('UNKNOWN')
            sales_df['cashier'] = sales_df['cashier'].fillna('UNKNOWN')
            sales_df['total'] = pd.to_numeric(sales_df['total'], errors='coerce').fillna(0)
            
            # Films table
            films_df = df[['film']].drop_duplicates()
            films_df = films_df[films_df['film'].notna()]  # Loại bỏ giá trị null
            films_df['film_id'] = range(1, len(films_df) + 1)
            
            # Rooms table
            rooms_df = df[['room']].drop_duplicates()
            rooms_df = rooms_df[rooms_df['room'].notna()]  # Loại bỏ giá trị null
            rooms_df['room_id'] = range(1, len(rooms_df) + 1)
            
            # Film-Room relationship table
            film_room_df = df[['film', 'room']].drop_duplicates()
            film_room_df = film_room_df[film_room_df['film'].notna() & film_room_df['room'].notna()]
            film_room_df = film_room_df.merge(films_df, on='film', how='inner')
            film_room_df = film_room_df.merge(rooms_df, on='room', how='inner')
            film_room_df = film_room_df[['film_id', 'room_id']]
            
            # Ticket types table
            if 'Thành viên' in df['ticket_type'].values:
                # Đếm số lượng thành viên duy nhất
                member_count = df[df['ticket_type'] == 'Thành viên']['customerid'].nunique()
                ticket_types_df = pd.DataFrame({
                    'ticket_type': ['Thành viên'],
                    'count': [member_count],
                    'type_id': [1]
                })
            else:
                ticket_types_df = df[['ticket_type']].drop_duplicates()
                ticket_types_df = ticket_types_df[ticket_types_df['ticket_type'].notna()]
                ticket_types_df['count'] = 0
                ticket_types_df['type_id'] = range(1, len(ticket_types_df) + 1)
            
            # Log thông tin về số lượng thành viên
            logger.info("\nTicket types distribution:")
            for _, row in ticket_types_df.iterrows():
                logger.info(f"{row['ticket_type']}: {row['count']} unique customers")
            
            # Seat types table
            seat_types_df = df[['slot_type']].drop_duplicates()
            seat_types_df = seat_types_df[seat_types_df['slot_type'].notna()]  # Loại bỏ giá trị null
            seat_types_df['seat_type_id'] = range(1, len(seat_types_df) + 1)
            
            # Log thông tin về các bảng
            logger.info(f"Tickets table: {len(tickets_df)} records")
            logger.info(f"Sales table: {len(sales_df)} records")
            logger.info(f"Films table: {len(films_df)} records")
            logger.info(f"Rooms table: {len(rooms_df)} records")
            logger.info(f"Film-Room relationship table: {len(film_room_df)} records")
            logger.info(f"Ticket types table: {len(ticket_types_df)} records")
            logger.info(f"Seat types table: {len(seat_types_df)} records")
            
            # Hiển thị mẫu dữ liệu của các bảng
            logger.info("Sample data from tickets table:")
            logger.info(tickets_df.head().to_string())
            
            logger.info("Sample data from films table:")
            logger.info(films_df.head().to_string())
            
            # Create datetime columns for time-based analysis
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df['hour'] = df['datetime'].dt.hour
            df['day'] = df['datetime'].dt.date
            df['month'] = df['datetime'].dt.month
            
            # Thêm các bảng phân tích theo thời gian
            # Hourly Analysis
            hourly_df = df.groupby('hour').agg({
                'orderid': 'count',
                'total': 'sum'
            }).reset_index()
            hourly_df.columns = ['hour', 'ticket_count', 'revenue']
            
            # Daily Analysis
            daily_df = df.groupby('day').agg({
                'orderid': 'count',
                'total': 'sum'
            }).reset_index()
            daily_df.columns = ['date', 'ticket_count', 'revenue']
            
            # Monthly Analysis
            monthly_df = df.groupby('month').agg({
                'orderid': 'count',
                'total': 'sum'
            }).reset_index()
            monthly_df.columns = ['month', 'ticket_count', 'revenue']
            
            # Log thông tin về các bảng mới
            logger.info(f"Hourly analysis table: {len(hourly_df)} records")
            logger.info(f"Daily analysis table: {len(daily_df)} records")
            logger.info(f"Monthly analysis table: {len(monthly_df)} records")
            
            return {
                'tickets': tickets_df,
                'sales': sales_df,
                'films': films_df,
                'rooms': rooms_df,
                'film_room': film_room_df,
                'ticket_types': ticket_types_df,
                'seat_types': seat_types_df,
                'hourly_analysis': hourly_df,
                'daily_analysis': daily_df,
                'monthly_analysis': monthly_df
            }
            
        except Exception as e:
            logger.error(f"Error converting to DB schema: {e}")
            logger.error("Error details:", exc_info=True)
            raise

    def save_to_csv(self, data, filename):
        try:
            filepath = os.path.join(self.output_dir, filename)
            
            # Kiểm tra dữ liệu trước khi lưu
            if data is None or len(data) == 0:
                logger.warning(f"No data to save for {filename}")
                return
                
            # Lưu dữ liệu
            data.to_csv(filepath, index=False)
            logger.info(f"Saved {filename}: {len(data)} records")
            
            # Kiểm tra file đã được tạo
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                logger.info(f"File {filename} created with size: {file_size} bytes")
                
                # Hiển thị một số dòng đầu tiên của file đã lưu
                if file_size > 0:
                    try:
                        sample_df = pd.read_csv(filepath, nrows=5)
                        logger.info(f"Sample data from saved file {filename}:")
                        logger.info(sample_df.to_string())
                    except Exception as e:
                        logger.error(f"Error reading sample from {filename}: {e}")
            else:
                logger.error(f"Failed to create file {filename}")
                
        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")
            logger.error("Error details:", exc_info=True)
            raise

    def generate_schema_documentation(self):
        """Generate documentation for the database schema"""
        schema_doc = """
Database Schema Documentation for Ticket System

1. Tickets Table
---------------
- orderid: Primary key
- customerid: Customer identifier
- ticketcode: Unique ticket code
- date: Show date
- time: Show time
- slot: Time slot
- room: Room identifier
- film: Film name
- slot_type: Type of seat
- ticket_type: Type of ticket
- ticket_price: Price of ticket
- popcorn: Popcorn included
- processed_at: Processing timestamp

2. Sales Table
-------------
- orderid: Foreign key to tickets.orderid
- cashier: Cashier identifier
- saledate: Sale date
- total: Total amount

3. Films Table
-------------
- film_id: Primary key
- film: Film name

4. Rooms Table
-------------
- room_id: Primary key
- room: Room name

5. Film-Room Relationship Table
-----------------------------
- film_id: Foreign key to films.film_id
- room_id: Foreign key to rooms.room_id

6. Ticket Types Table
-------------------
- type_id: Primary key
- ticket_type: Type of ticket
- count: Number of unique customers for this ticket type

7. Seat Types Table
-----------------
- seat_type_id: Primary key
- slot_type: Type of seat

8. Hourly Analysis Table
----------------------
- hour: Hour of the day (0-23)
- ticket_count: Number of tickets sold in this hour
- revenue: Total revenue in this hour

9. Daily Analysis Table
---------------------
- date: Date of sales
- ticket_count: Number of tickets sold on this date
- revenue: Total revenue on this date

10. Monthly Analysis Table
------------------------
- month: Month number (1-12)
- ticket_count: Number of tickets sold in this month
- revenue: Total revenue in this month

11. Film Detailed Analysis Table
----------------------------
- film: Film name
- total_tickets: Total number of tickets sold
- total_revenue: Total revenue from ticket sales
- avg_ticket_price: Average ticket price
- single_seats: Number of single seat tickets
- double_seats: Number of double seat tickets
- total_seats: Total number of seats (single + double)
- revenue_per_seat: Average revenue per seat
"""
        try:
            doc_path = os.path.join(self.output_dir, 'schema_documentation.txt')
            with open(doc_path, 'w', encoding='utf-8') as f:
                f.write(schema_doc)
            logger.info("Generated schema documentation")
        except Exception as e:
            logger.error(f"Error generating documentation: {e}")

    def create_film_detailed_analysis(self, df):
        try:
            # Group by film and calculate statistics
            film_analysis = df.groupby('film').agg({
                'ticket_price': ['count', 'sum', 'mean'],
                'customerid': 'nunique',  # Count unique customers
            }).reset_index()

            # Flatten column names
            film_analysis.columns = [
                'film', 'total_tickets', 'total_revenue', 'avg_ticket_price', 'total_customers'
            ]
            
            # Calculate seat distribution
            seat_dist = df.groupby(['film', 'slot_type']).size().unstack(fill_value=0)
            if 'ĐƠN' not in seat_dist.columns:
                seat_dist['ĐƠN'] = 0
            if 'ĐÔI' not in seat_dist.columns:
                seat_dist['ĐÔI'] = 0
            
            # Merge seat distribution with main analysis
            film_analysis = film_analysis.merge(
                seat_dist[['ĐƠN', 'ĐÔI']], 
                left_on='film', 
                right_index=True
            )
            
            # Rename seat columns
            film_analysis = film_analysis.rename(columns={
                'ĐƠN': 'single_seats',
                'ĐÔI': 'double_seats'
            })
            
            # Calculate additional metrics
            film_analysis['total_seats'] = film_analysis['single_seats'] + (film_analysis['double_seats'] * 2)
            film_analysis['tickets_per_customer'] = film_analysis['total_tickets'] / film_analysis['total_customers']
            film_analysis['revenue_per_ticket'] = film_analysis['total_revenue'] / film_analysis['total_tickets']
            
            # Create seat distribution string
            film_analysis['seat_distribution'] = film_analysis.apply(
                lambda x: f"Single: {x['single_seats']}, Double: {x['double_seats']}", axis=1
            )
            
            # Round numeric columns
            numeric_columns = [
                'total_revenue', 'avg_ticket_price', 'tickets_per_customer',
                'revenue_per_ticket'
            ]
            film_analysis[numeric_columns] = film_analysis[numeric_columns].round(2)
            
            # Reorder columns
            columns_order = [
                'film', 'total_tickets', 'total_customers', 'tickets_per_customer',
                'total_revenue', 'avg_ticket_price', 'revenue_per_ticket', 'seat_distribution'
            ]
            film_analysis = film_analysis[columns_order]
            
            # Save to CSV
            self.save_to_csv(film_analysis, 'film_detailed_analysis.csv')
            
            # Log sample data
            logger.info("\nFilm Detailed Analysis Sample:")
            logger.info(film_analysis.head().to_string())
            
        except Exception as e:
            logger.error(f"Error creating film detailed analysis: {e}")
            logger.error("Error details:", exc_info=True)

    def process_data(self):
        try:
            # Read the processed data
            df = self.read_processed_data()
            
            # Convert to DB schema
            db_tables = self.convert_to_db_schema(df)
            
            # Save each table
            self.save_to_csv(db_tables['tickets'], 'tickets.csv')
            self.save_to_csv(db_tables['sales'], 'sales.csv')
            self.save_to_csv(db_tables['films'], 'films.csv')
            self.save_to_csv(db_tables['rooms'], 'rooms.csv')
            self.save_to_csv(db_tables['film_room'], 'film_room.csv')
            self.save_to_csv(db_tables['ticket_types'], 'ticket_types.csv')
            self.save_to_csv(db_tables['seat_types'], 'seat_types.csv')
            
            # Save time-based analysis tables
            self.save_to_csv(db_tables['hourly_analysis'], 'hourly_analysis.csv')
            self.save_to_csv(db_tables['daily_analysis'], 'daily_analysis.csv')
            self.save_to_csv(db_tables['monthly_analysis'], 'monthly_analysis.csv')
            
            # Create film detailed analysis
            self.create_film_detailed_analysis(df)
            
            # Generate schema documentation
            self.generate_schema_documentation()
            
            logger.info("Data conversion completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            logger.error("Error details:", exc_info=True)
            raise

def main():
    try:
        # Initialize the converter
        converter = DBSchemaConverter()
        
        # Process the data
        converter.process_data()
        
        logger.info("Script completed successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        logger.error("Error details:", exc_info=True)

if __name__ == "__main__":
    main() 