import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# Database Connection
DB_NAME = "stock_db"
DB_USER = "postgres"
DB_PASS = "24081314"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def etl_pipeline(stock_symbol):
    
    # ✅ Extract: Get historical stock data from Yahoo Finance
    stock = yf.Ticker(stock_symbol)
    df = stock.history(period="1mo", interval="1d")  # Last month, daily data
    
    # ✅ Transform: Process Data
    df.reset_index(inplace=True)
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df['Date'] = pd.to_datetime(df['Date'])
    
    # ✅ Load: Save to PostgreSQL
    df.to_sql('stocks_data', con=engine, if_exists='replace', index=False)
    print(f"✅ Data for {stock_symbol} saved to PostgreSQL!")

# Run ETL for Apple (AAPL)
if __name__ == "__main__":
    etl_pipeline("AAPL")
