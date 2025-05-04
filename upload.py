from pathlib import Path
import time
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import re

# Update project and dataset information
project_id = 'iconic-valve-446108-v7'
dataset_id = 'ticket_data'

def table_reference(project_id, dataset_id, table_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)
    return table_ref

def extract_seat_numbers(seat_distribution):
    """Extract single and double seat numbers from seat distribution string"""
    single_match = re.search(r'Single: (\d+)', seat_distribution)
    double_match = re.search(r'Double: (\d+)', seat_distribution)
    single_seats = int(single_match.group(1)) if single_match else 0
    double_seats = int(double_match.group(1)) if double_match else 0
    return single_seats, double_seats

def get_schema(table_name):
    """Return the schema for a specific table"""
    schemas = {
        'tickets': [
            bigquery.SchemaField('orderid', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('customerid', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ticketcode', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('time', 'TIME', mode='NULLABLE'),
            bigquery.SchemaField('slot', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('room', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('film', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('slot_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ticket_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ticket_price', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('popcorn', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('processed_at', 'TIMESTAMP', mode='NULLABLE')
        ],
        'sales': [
            bigquery.SchemaField('orderid', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('cashier', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('saledate', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('total', 'FLOAT', mode='NULLABLE')
        ],
        'films': [
            bigquery.SchemaField('film_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('film', 'STRING', mode='NULLABLE')
        ],
        'rooms': [
            bigquery.SchemaField('room_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('room', 'STRING', mode='NULLABLE')
        ],
        'film_room': [
            bigquery.SchemaField('film_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('room_id', 'STRING', mode='NULLABLE')
        ],
        'ticket_types': [
            bigquery.SchemaField('type_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ticket_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('count', 'INTEGER', mode='NULLABLE')
        ],
        'seat_types': [
            bigquery.SchemaField('seat_type_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('slot_type', 'STRING', mode='NULLABLE')
        ],
        'hourly_analysis': [
            bigquery.SchemaField('hour', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('ticket_count', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('revenue', 'FLOAT', mode='NULLABLE')
        ],
        'daily_analysis': [
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('ticket_count', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('revenue', 'FLOAT', mode='NULLABLE')
        ],
        'monthly_analysis': [
            bigquery.SchemaField('month', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('ticket_count', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('revenue', 'FLOAT', mode='NULLABLE')
        ],
        'film_detailed_analysis': [
            bigquery.SchemaField('film', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('total_tickets', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('total_customers', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('tickets_per_customer', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('total_revenue', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('avg_ticket_price', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('revenue_per_ticket', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('single_seats', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('double_seats', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('total_seats', 'INTEGER', mode='NULLABLE')
        ]
    }
    return schemas.get(table_name)

def upload_csv(client, table_ref, csv_file, table_name):
    try:
        print(f"\nStarting upload for {table_name}...")
        
        # Delete table if it exists
        client.delete_table(table_ref, not_found_ok=True)
        print(f"Deleted existing table {table_name} if it existed")

        # Get schema for the specific table
        schema = get_schema(table_name)
        if not schema:
            print(f"Schema not found for table {table_name}")
            return

        # Configure load job
        load_job_configuration = bigquery.LoadJobConfig()
        load_job_configuration.schema = schema
        load_job_configuration.source_format = bigquery.SourceFormat.CSV
        load_job_configuration.skip_leading_rows = 1
        load_job_configuration.allow_quoted_newlines = True
        load_job_configuration.allow_jagged_rows = True
        load_job_configuration.ignore_unknown_values = True

        # Special handling for different tables
        if table_name == 'sales':
            print("Processing sales table with special handling...")
            df = pd.read_csv(csv_file)
            print(f"Read {len(df)} rows from sales.csv")
            df['total'] = pd.to_numeric(df['total'], errors='coerce')
            temp_csv = csv_file.parent / f'temp_{csv_file.name}'
            df.to_csv(temp_csv, index=False)
            csv_file = temp_csv
            print("Created temporary CSV file for sales")
        elif table_name == 'film_detailed_analysis':
            print("Processing film_detailed_analysis table with special handling...")
            df = pd.read_csv(csv_file)
            print(f"Read {len(df)} rows from film_detailed_analysis.csv")
            
            # Extract seat numbers from seat_distribution
            df['single_seats'] = df['seat_distribution'].apply(lambda x: extract_seat_numbers(x)[0])
            df['double_seats'] = df['seat_distribution'].apply(lambda x: extract_seat_numbers(x)[1])
            df['total_seats'] = df['single_seats'] + df['double_seats']
            
            # Drop the original seat_distribution column
            df = df.drop('seat_distribution', axis=1)
            
            # Convert numeric columns
            numeric_columns = ['total_tickets', 'total_customers', 'tickets_per_customer', 
                             'total_revenue', 'avg_ticket_price', 'revenue_per_ticket',
                             'single_seats', 'double_seats', 'total_seats']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            temp_csv = csv_file.parent / f'temp_{csv_file.name}'
            df.to_csv(temp_csv, index=False)
            csv_file = temp_csv
            print("Created temporary CSV file for film_detailed_analysis")

        # Upload data from CSV to BigQuery
        print(f"Starting upload to BigQuery for {table_name}...")
        with open(csv_file, 'rb') as source_file:
            upload_job = client.load_table_from_file(
                source_file,
                destination=table_ref,
                location='US',
                job_config=load_job_configuration
            )

        # Wait for upload to complete
        print(f"Waiting for {table_name} upload to complete...")
        while upload_job.state != 'DONE':
            time.sleep(2)
            upload_job.reload()
            print(f"Current state: {upload_job.state}")

        result = upload_job.result()
        print(f"Upload completed for {table_name}: {result}")

        # Clean up temporary file if it exists
        if 'temp_csv' in locals():
            temp_csv.unlink()
            print("Cleaned up temporary CSV file")

    except Exception as e:
        print(f"\nError uploading {table_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        if hasattr(e, 'errors'):
            print("Detailed errors:")
            for error in e.errors:
                print(error)
        if 'temp_csv' in locals():
            temp_csv.unlink()
            print("Cleaned up temporary file after error")

def main():
    # Initialize BigQuery Client
    client = bigquery.Client(project=project_id)
    print(f"Connected to BigQuery project: {project_id}")

    # Path to the folder containing CSV files
    data_file_folder = Path('db_schema_ticket_data')
    print(f"Looking for CSV files in: {data_file_folder}")

    # List of CSV files to process
    csv_files = [
        'tickets.csv',
        'sales.csv',
        'films.csv',
        'rooms.csv',
        'film_room.csv',
        'ticket_types.csv',
        'seat_types.csv',
        'hourly_analysis.csv',
        'daily_analysis.csv',
        'monthly_analysis.csv',
        'film_detailed_analysis.csv'
    ]

    # Process each CSV file
    for csv_file_name in csv_files:
        csv_file = data_file_folder.joinpath(csv_file_name)
        if csv_file.exists():
            print(f'\nProcessing file: {csv_file.name}')
            table_name = csv_file.stem
            table_ref = table_reference(project_id, dataset_id, table_name)
            upload_csv(client, table_ref, csv_file, table_name)
        else:
            print(f'File {csv_file_name} not found in the specified directory.')

if __name__ == "__main__":
    main()
# (Tùy chọn) Xóa các bảng trong dataset nếu cần
# delete_dataset_tables(client, project_id, dataset_id)