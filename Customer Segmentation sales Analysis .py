import pandas as pd
df = pd.read_csv(r'D:\Data\Customer shopping data.csv')

df.head()

df.info()

# Summary statistics using .describe()
df.describe(include='all')

# Checking if missing data or null values are present in the dataset
df.isnull().sum()

df.isnull().sum()

# Renaming columns according to snake casing for better readability and documentation
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

df.columns

# create a new column age_group
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels = labels)

df[['age','age_group']].head(10)

# create new column purchase_frequency_days
frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-Weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

df[['purchase_frequency_days','frequency_of_purchases']].head(10)

df[['discount_applied','promo_code_used']].head(10)

(df['discount_applied'] == df['promo_code_used']).all()

# Dropping promo code used column

df = df.drop('promo_code_used', axis=1)

df.columns
Connecting Python script to PostgreSQL

#install python packages and connect with database
!pip install psycopg2-binary sqlalchemy

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

USER     = "#####"       
PASSWORD = "#####" 
HOST     = "######"
PORT     = "#####"
DATABASE = "Customer_Segmentation_Sales_Trend_Analysis"
CSV_PATH = r"D:\Data\Customer shopping data.csv"

#Create the database if it doesn't exist 
conn = psycopg2.connect(
    user=USER, password=PASSWORD,
    host=HOST, port=PORT,
    database="postgres"         
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,))
if not cursor.fetchone():
    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE)))
    print(f"Database '{DATABASE}' created successfully.")
else:
    print(f"Database '{DATABASE}' already exists.")

cursor.close()
conn.close()

#Load CSV
df = pd.read_csv(CSV_PATH)
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace(r"[()]", "", regex=True)
)
print("CSV loaded:", df.shape)

#Connect to the new database & load data
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

df.to_sql("customer", engine, if_exists="replace", index=False)
print(" Data successfully loaded into table 'customer' in database 'Customer_Segmentation_Sales_Trend_Analysis'.")