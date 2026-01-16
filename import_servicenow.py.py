import pandas as pd
from datasets import load_dataset
import mysql.connector
from mysql.connector import Error

# Step 1: Load the dataset
ds = load_dataset("6StringNinja/synthetic-servicenow-incidents")
df = ds['train'].to_pandas()  # Convert to pandas DataFrame


# Step 2: Clean/prepare data for MySQL
# Check column types and handle any issues
print(df.info())
print(df.head())


# Step 3: Direct MySQL Import
import pandas as pd
from datasets import load_dataset
import mysql.connector


print("Testing dataset load...")
try:
    ds = load_dataset("6StringNinja/synthetic-servicenow-incidents")
    df = ds['train'].to_pandas()
    print(f"✓ Dataset loaded successfully")
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    print(f"  First few columns: {df.columns[:5].tolist()}")
except Exception as e:
    print(f"✗ Error loading dataset: {e}")
    exit()

# Test MySQL connection separately
print("\nTesting MySQL connection...")
import mysql.connector

# Common host values to try:
host_options = [
    'localhost',
    '127.0.0.1',
    'local',  # Only if you specifically named it this
    'mysql',  # For Docker containers
    '127.0.0.1:3306',  # With port
]

for host in host_options:
    try:
        print(f"Trying to connect to host: {host}")
        connection = mysql.connector.connect(
            host=host,
            user='root',
            password='Welcome@1',
            database='testdb'  # or don't specify database initially
        )
        print(f"✓ Successfully connected to {host}")
        connection.close()
        break
    except mysql.connector.Error as err:
        print(f"✗ Failed to connect to {host}: {err}")
    exit()

# Prepare data
df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
print(f"\nColumns after cleaning: {df.columns.tolist()}")



import pandas as pd
from datasets import load_dataset
import mysql.connector
from mysql.connector import Error

# Initialize variables
conn = None
cursor = None

try:
    # Step 1: Load the dataset
    print("Loading dataset from Hugging Face...")
    ds = load_dataset("6StringNinja/synthetic-servicenow-incidents")
    df = ds['train'].to_pandas()
    
    print(f"Dataset loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    
    # Step 2: Clean column names for MySQL
    df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
    print(f"Cleaned columns: {df.columns.tolist()}")
    
    # Step 3: Connect to MySQL
    print("\nConnecting to MySQL...")
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Welcome@1',
        database='testdb'
    )
    
    # Now create cursor after successful connection
    cursor = conn.cursor()
    print("✓ Connected to MySQL successfully")
    
    # Step 4: Create table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS servicenow_incidents (
        id INT AUTO_INCREMENT PRIMARY KEY,
        number VARCHAR(100),
        short_description TEXT,
        description LONGTEXT,
        urgency VARCHAR(50),
        impact VARCHAR(50),
        category VARCHAR(100),
        assignment_group VARCHAR(200),
        resolution TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_sql)
    print("✓ Table created successfully")
    
    # Step 5: Insert data
    print(f"\nInserting {len(df)} records...")
    
    # Truncate table first to avoid duplicates
    cursor.execute("TRUNCATE TABLE servicenow_incidents")
    
    for index, row in df.iterrows():
        insert_sql = """
        INSERT INTO servicenow_incidents 
        (number, short_description, description, urgency, impact, category, assignment_group, resolution)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Handle missing values safely
        values = (
            str(row.get('number', '')) if pd.notna(row.get('number')) else '',
            str(row.get('short_description', '')) if pd.notna(row.get('short_description')) else '',
            str(row.get('description', '')) if pd.notna(row.get('description')) else '',
            str(row.get('urgency', '')) if pd.notna(row.get('urgency')) else '',
            str(row.get('impact', '')) if pd.notna(row.get('impact')) else '',
            str(row.get('category', '')) if pd.notna(row.get('category')) else '',
            str(row.get('assignment_group', '')) if pd.notna(row.get('assignment_group')) else '',
            str(row.get('resolution', '')) if pd.notna(row.get('resolution')) else ''
        )
        
        cursor.execute(insert_sql, values)
        
        # Progress indicator
        if (index + 1) % 100 == 0:
            print(f"  Inserted {index + 1} records...")
    
    conn.commit()
    print(f"\n✓ Successfully imported {len(df)} records")
    
    # Step 6: Verify
    cursor.execute("SELECT COUNT(*) FROM servicenow_incidents")
    count = cursor.fetchone()[0]
    print(f"Total records in database: {count}")
    
    cursor.execute("SELECT * FROM servicenow_incidents LIMIT 5")
    sample = cursor.fetchall()
    print("\nSample records:")
    for row in sample:
        print(row)

except Error as e:
    print(f"✗ MySQL Error: {e}")
except Exception as e:
    print(f"✗ General Error: {e}")
    print(f"Error type: {type(e).__name__}")
  
    
 # Verify
    print("\nStep 5: Verifying data...")
    cursor.execute("SELECT COUNT(*) FROM servicenow_incidents")
    count = cursor.fetchone()[0]
    print(f"Total in table: {count} records")
    
    cursor.execute("SELECT number, short_description FROM servicenow_incidents LIMIT 5")
    print("\nFirst 5 records:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1][:50]}...")
    
    # Cleanup
    cursor.close()
    conn.close()
    print("\n✓ All done! Data is ready in MySQL Workbench")