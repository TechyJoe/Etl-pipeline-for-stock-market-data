import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database Connection
DB_NAME = "stock_db"
DB_USER = "postgres"
DB_PASS = "24081314"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Fetch Data
def get_stock_data():
    query = "SELECT * FROM stocks_data ORDER BY Date DESC LIMIT 100"
    return pd.read_sql(query, engine)

st.title("📈 Stock Market Dashboard")
st.sidebar.header("Select Options")

# Fetch and display data
df = get_stock_data()
st.line_chart(df.set_index("Date")["Close"])

st.write(df.head())  # Show data table
