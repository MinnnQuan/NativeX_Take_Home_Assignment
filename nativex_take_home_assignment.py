import pandas as pd
import math
import psycopg2
from sqlalchemy import create_engine


# . Data Ingestion
# Import dataset into Python env
file_path = r'tripadvisor_review.csv'
df = pd.read_csv(file_path)
print('Original DataFrame: ', df.head())


# . Data Transformation
# find missing values
print(df.isnull().sum())
print(df.shape)
# drop missing values
df_dropped = df.dropna(how= 'any')

# Generate summary statistics
summary_X = df['Category 1'].describe()
summary_Y = df['Category 10'].describe()
summary_Z = df['Category 4'].describe()

# Display summary statistics
print(f"\nSummary Statistics for X:\n{summary_X}")
print(f"\nSummary Statistics for Y:\n{summary_Y}")
print(f"\nSummary Statistics for Z:\n{summary_Z}")

# store cleaned data to Postgres.

db_params = {
    'host': 'localhost',
    'database': 'nativex',
    'user': 'admin',
    'password': 'P@ssw0rd',
}


conn = psycopg2.connect(**db_params)

create_table_query = """
CREATE TABLE tripadvisor_review (
    UserID varchar,
    Category1 DECIMAL,
    Category2 DECIMAL,
    Category3 DECIMAL,
    Category4 DECIMAL,
    Category5 DECIMAL,
    Category6 DECIMAL,
    Category7 DECIMAL,
    Category8 DECIMAL,
    Category9 DECIMAL,
    Category10 DECIMAL
);
"""
with conn.cursor() as cursor:
    cursor.execute(create_table_query)


# Store cleaned data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}")
df.to_sql('tripadvisor_review', engine, if_exists='replace', index=False)

# Write SQL queries
query1 = "SELECT COUNT(DISTINCT X) FROM tripadvisor_review;"
query2 = "SELECT Z, AVG(Y) FROM tripadvisor_review GROUP BY Z;"

with conn.cursor() as cursor:
    cursor.execute(query1)
    result1 = cursor.fetchone()
    print(f"\nNumber of unique values in variable X: {result1[0]}")

    cursor.execute(query2)
    result2 = cursor.fetchall()
    print("\nAverage of variable Y grouped by variable Z:")
    for row in result2:
        print(f"{row[0]}: {row[1]}")

# Close the connection
conn.close()


