import mysql.connector

# Connect to MySQL server
conn = mysql.connector.connect(
    host='localhost',
    user='root',          
    password='1234'  
)

cursor = conn.cursor()

# Create a database
cursor.execute("CREATE DATABASE IF NOT EXISTS agri_data;")
print("✅ Database 'agri_data' created successfully!")

cursor.close()
conn.close()
