import pandas as pd
from sqlalchemy import create_engine

# Loading cleaned data
df = pd.read_csv('Cleaned_AgriData.csv')

# Database connection
user = 'root'
password = '1234'
host = 'localhost'
database = 'agri_data'

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Upload data to MySQL
df.to_sql('agriculture', con=engine, if_exists='replace', index=False)

print("✅ Cleaned data uploaded to MySQL database successfully!")
