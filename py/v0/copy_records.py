import sqlite3

# Configuration
source_db = './curate1.db'
target_db = '../curate1.db'
table_name = 'attributes'
condition = "label = 'hn_content'"  # Replace with your condition

# Step 1: Fetch the subset of data from the source database
source_conn = sqlite3.connect(source_db)
source_cursor = source_conn.cursor()
query = f"SELECT * FROM {table_name} WHERE {condition}"
print(f"executing query: {query}")
source_cursor.execute(query)
rows = source_cursor.fetchall()
print(f"got {len(rows)} rows")
source_conn.close()

# Step 2: Insert the subset of data into the target database
target_conn = sqlite3.connect(target_db)
target_cursor = target_conn.cursor()
for row in rows:
  values = ', '.join(map(lambda x: f"'{x}'" if isinstance(x, str) else str(x), row))
  insert_query = f"INSERT INTO {table_name} VALUES ({values});"
  print(f"executing insert query: {insert_query}")
  target_cursor.execute(insert_query)
target_conn.commit()
target_conn.close()

