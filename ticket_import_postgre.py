import psycopg2
import pandas as pd
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PostgresImporter:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'ticket_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '1'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.input_dir = 'db_schema_ticket_data'
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def create_tables(self):
        """Create database tables if they don't exist"""
        try:
            # Drop existing tables if they exist
            self.cur.execute("DROP TABLE IF EXISTS seat_types CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS tickets CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS film_room CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS hourly_analysis CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS daily_analysis CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS monthly_analysis CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS films CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS rooms CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS ticket_types CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS film_detailed_analysis CASCADE")
            self.cur.execute("DROP TABLE IF EXISTS film_distribution CASCADE")

            # Create films table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS films (
                    film_id INTEGER PRIMARY KEY,
                    film VARCHAR(255) NOT NULL
                )
            """)

            # Create rooms table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS rooms (
                    room_id INTEGER PRIMARY KEY,
                    room VARCHAR(50) NOT NULL
                )
            """)

            # Create seat_types table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS seat_types (
                    seat_type_id INTEGER PRIMARY KEY,
                    slot_type VARCHAR(50) NOT NULL
                )
            """)

            # Create ticket_types table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS ticket_types (
                    type_id INTEGER PRIMARY KEY,
                    ticket_type VARCHAR(50) NOT NULL,
                    count INTEGER
                )
            """)

            # Create tickets table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    orderid VARCHAR(50) PRIMARY KEY,
                    customerid VARCHAR(50),
                    ticketid INTEGER,
                    ticketcode VARCHAR(50),
                    date DATE,
                    time TIME,
                    slot VARCHAR(50),
                    room VARCHAR(50),
                    room_id INTEGER REFERENCES rooms(room_id),
                    film VARCHAR(255),
                    film_id INTEGER REFERENCES films(film_id),
                    slot_type VARCHAR(50),
                    seat_type_id INTEGER REFERENCES seat_types(seat_type_id),
                    ticket_type VARCHAR(50),
                    ticket_type_id INTEGER REFERENCES ticket_types(type_id),
                    ticket_price DECIMAL(10,2),
                    popcorn VARCHAR(50),
                    processed_at TIMESTAMP,
                    sale_date TIMESTAMP,
                    price DECIMAL(10,2)
                )
            """)

            # Create film_room table for film-room relationships
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS film_room (
                    film_id INTEGER REFERENCES films(film_id),
                    room_id INTEGER REFERENCES rooms(room_id),
                    PRIMARY KEY (film_id, room_id)
                )
            """)

            # Create hourly_analysis table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_analysis (
                    hour INTEGER PRIMARY KEY,
                    ticket_count INTEGER NOT NULL,
                    revenue DECIMAL(15,2) NOT NULL
                )
            """)

            # Create daily_analysis table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_analysis (
                    date DATE PRIMARY KEY,
                    ticket_count INTEGER NOT NULL,
                    revenue DECIMAL(15,2) NOT NULL
                )
            """)

            # Create monthly_analysis table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS monthly_analysis (
                    month INTEGER PRIMARY KEY,
                    ticket_count INTEGER NOT NULL,
                    revenue DECIMAL(15,2) NOT NULL
                )
            """)

            # Create film_detailed_analysis table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS film_detailed_analysis (
                    film VARCHAR(255) PRIMARY KEY,
                    total_tickets INTEGER NOT NULL,
                    total_customers INTEGER NOT NULL,
                    tickets_per_customer DECIMAL(10,2) NOT NULL,
                    total_revenue DECIMAL(15,2) NOT NULL,
                    avg_ticket_price DECIMAL(10,2) NOT NULL,
                    revenue_per_ticket DECIMAL(10,2) NOT NULL,
                    seat_distribution TEXT NOT NULL
                )
            """)

            # Create film_distribution table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS film_distribution (
                    film VARCHAR(255) PRIMARY KEY,
                    count INTEGER NOT NULL
                )
            """)

            self.conn.commit()
            logger.info("Successfully created database tables")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error creating tables: {e}")
            raise

    def import_data(self, table_name, file_path):
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Convert DataFrame to list of tuples using itertuples
            records = [tuple(row[1:]) for row in df.itertuples()]
            
            # Get column names
            columns = ', '.join(df.columns)
            
            # Create placeholders for values
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            # Create insert query
            query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            
            # Execute batch insert
            self.cur.executemany(query, records)
            self.conn.commit()
            
            logger.info(f"Successfully imported {len(records)} records into {table_name}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error importing data to {table_name}: {e}")
            raise

    def import_all_data(self):
        try:
            # Import films first
            films_path = os.path.join(self.input_dir, 'films.csv')
            self.import_data('films', films_path)
            
            # Import rooms
            rooms_path = os.path.join(self.input_dir, 'rooms.csv')
            self.import_data('rooms', rooms_path)
            
            # Import seat types
            seat_types_path = os.path.join(self.input_dir, 'seat_types.csv')
            self.import_data('seat_types', seat_types_path)
            
            # Import ticket types
            ticket_types_path = os.path.join(self.input_dir, 'ticket_types.csv')
            self.import_data('ticket_types', ticket_types_path)
            
            # Import film-room relationships
            film_room_path = os.path.join(self.input_dir, 'film_room.csv')
            self.import_data('film_room', film_room_path)
            
            # Import tickets last (due to foreign key dependencies)
            tickets_path = os.path.join(self.input_dir, 'tickets.csv')
            self.import_data('tickets', tickets_path)
            
            # Import time-based analysis tables
            hourly_path = os.path.join(self.input_dir, 'hourly_analysis.csv')
            self.import_data('hourly_analysis', hourly_path)
            
            daily_path = os.path.join(self.input_dir, 'daily_analysis.csv')
            self.import_data('daily_analysis', daily_path)
            
            monthly_path = os.path.join(self.input_dir, 'monthly_analysis.csv')
            self.import_data('monthly_analysis', monthly_path)
            
            # Import film detailed analysis
            film_detailed_path = os.path.join(self.input_dir, 'film_detailed_analysis.csv')
            self.import_data('film_detailed_analysis', film_detailed_path)
            
            # Import film distribution
            film_distribution_path = os.path.join('processed_ticket_data', 'film_distribution.csv')
            self.import_data('film_distribution', film_distribution_path)
            
            logger.info("Successfully imported all data")
        except Exception as e:
            logger.error(f"Error importing data: {e}")
            raise

    def verify_import(self):
        try:
            # Check record counts
            tables = [
                'films', 'rooms', 'seat_types', 'ticket_types', 
                'film_room', 'tickets', 'hourly_analysis', 
                'daily_analysis', 'monthly_analysis',
                'film_detailed_analysis', 'film_distribution'
            ]
            for table in tables:
                self.cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cur.fetchone()[0]
                logger.info(f"Table {table} has {count} records")
            
            # Check for any foreign key violations in tickets table
            self.cur.execute("""
                SELECT COUNT(*) 
                FROM tickets t
                LEFT JOIN films f ON t.film_id = f.film_id
                LEFT JOIN rooms r ON t.room_id = r.room_id
                LEFT JOIN seat_types st ON t.seat_type_id = st.seat_type_id
                LEFT JOIN ticket_types tt ON t.ticket_type_id = tt.type_id
                WHERE f.film_id IS NULL 
                   OR r.room_id IS NULL 
                   OR st.seat_type_id IS NULL 
                   OR tt.type_id IS NULL
            """)
            violations = self.cur.fetchone()[0]
            if violations > 0:
                logger.warning(f"Found {violations} foreign key violations in tickets table")
            else:
                logger.info("No foreign key violations found")
            
            # Verify time-based analysis data
            self.cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM hourly_analysis) as hourly_count,
                    (SELECT COUNT(*) FROM daily_analysis) as daily_count,
                    (SELECT COUNT(*) FROM monthly_analysis) as monthly_count,
                    (SELECT SUM(ticket_count) FROM hourly_analysis) as total_hourly_tickets,
                    (SELECT SUM(ticket_count) FROM daily_analysis) as total_daily_tickets,
                    (SELECT SUM(ticket_count) FROM monthly_analysis) as total_monthly_tickets
            """)
            counts = self.cur.fetchone()
            logger.info("\nTime-based Analysis Verification:")
            logger.info(f"Hourly records: {counts[0]}")
            logger.info(f"Daily records: {counts[1]}")
            logger.info(f"Monthly records: {counts[2]}")
            logger.info(f"Total hourly tickets: {counts[3]}")
            logger.info(f"Total daily tickets: {counts[4]}")
            logger.info(f"Total monthly tickets: {counts[5]}")
            
        except Exception as e:
            logger.error(f"Error verifying import: {e}")
            raise

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

def main():
    importer = PostgresImporter()
    try:
        # Connect to database
        importer.connect()
        
        # Create tables
        importer.create_tables()
        
        # Import data
        importer.import_all_data()
        
        # Verify import
        importer.verify_import()
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        importer.close()

if __name__ == "__main__":
    main() 