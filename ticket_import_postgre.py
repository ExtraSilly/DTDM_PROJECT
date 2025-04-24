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
                    type_id INTEGER PRIMARY KEY,
                    seat_type VARCHAR(50) NOT NULL,
                    price DECIMAL(10,2) NOT NULL
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
                    ticket_id SERIAL PRIMARY KEY,
                    film_id INTEGER REFERENCES films(film_id),
                    room_id INTEGER REFERENCES rooms(room_id),
                    seat_type_id INTEGER REFERENCES seat_types(type_id),
                    ticket_type_id INTEGER REFERENCES ticket_types(type_id),
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
            
            # Convert DataFrame to list of tuples
            records = [tuple(x) for x in df.to_numpy()]
            
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
            
            logger.info("Successfully imported all data")
        except Exception as e:
            logger.error(f"Error importing data: {e}")
            raise

    def verify_import(self):
        try:
            # Check record counts
            tables = ['films', 'rooms', 'seat_types', 'ticket_types', 'film_room', 'tickets']
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
                LEFT JOIN seat_types st ON t.seat_type_id = st.type_id
                LEFT JOIN ticket_types tt ON t.ticket_type_id = tt.type_id
                WHERE f.film_id IS NULL 
                   OR r.room_id IS NULL 
                   OR st.type_id IS NULL 
                   OR tt.type_id IS NULL
            """)
            violations = self.cur.fetchone()[0]
            if violations > 0:
                logger.warning(f"Found {violations} foreign key violations in tickets table")
            else:
                logger.info("No foreign key violations found")
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