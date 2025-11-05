import psycopg2
import random
import time
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import config

MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def connect_to_db():
    params = config()
    return psycopg2.connect(**params)

def get_next_date(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT year_, month_ FROM Prices ORDER BY year_ DESC, CASE month_ WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3 WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6 WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9 WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12 END DESC LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            year_, month_ = result
            month_index = MONTHS.index(month_)
            
            if month_index == 11:
                next_year = year_ + 1
                next_month = MONTHS[0]
            else:
                next_year = year_
                next_month = MONTHS[month_index + 1]
            
            cursor.execute("SELECT COUNT(*) FROM Prices WHERE year_ = %s AND month_ = %s", (next_year, next_month))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                return find_next_available_date(conn, next_year, next_month)
            else:
                return next_year, next_month
        else:
            return 2021, 'Dec'
    except Exception as e:
        print(f"Error getting last date: {e}")
        return 2021, 'Dec'
    finally:
        cursor.close()

def find_next_available_date(conn, start_year, start_month):
    cursor = conn.cursor()
    try:
        year = start_year
        month_index = MONTHS.index(start_month)
        
        for _ in range(24):
            cursor.execute("SELECT COUNT(*) FROM Prices WHERE year_ = %s AND month_ = %s", (year, MONTHS[month_index]))
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                return year, MONTHS[month_index]
            
            month_index += 1
            if month_index == 12:
                month_index = 0
                year += 1
        
        return year, MONTHS[month_index]
    except Exception as e:
        print(f"Error finding available date: {e}")
        return datetime.now().year, MONTHS[datetime.now().month - 1]
    finally:
        cursor.close()

def generate_price_data(year_, month_):
    base_price = random.uniform(35.0, 55.0)
    volatility = random.uniform(-0.15, 0.15)
    price_uranium = base_price * (1 + volatility)
    inflation_rate = random.uniform(2.0, 8.0)
    price_uran_inflation = price_uranium * (1 + inflation_rate / 100)
    
    return {
        'year_': year_,
        'month_': month_,
        'price_uranium': round(price_uranium, 2),
        'inflation_rate': round(inflation_rate, 2),
        'price_uran_inflation': round(price_uran_inflation, 6)
    }

def insert_price_record(conn):
    cursor = conn.cursor()
    
    try:
        year_, month_ = get_next_date(conn)
        data = generate_price_data(year_, month_)
        
        insert_query = """
        INSERT INTO Prices (year_, month_, price_uranium, inflation_rate, price_uran_inflation)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            data['year_'],
            data['month_'],
            data['price_uranium'],
            data['inflation_rate'],
            data['price_uran_inflation']
        ))
        
        conn.commit()
        print(f"Added: {data['year_']} {data['month_']} - Price: ${data['price_uranium']}, Inflation: {data['inflation_rate']}%")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    conn = connect_to_db()
    record_count = 0
    
    try:
        while True:
            interval = random.uniform(10, 15)
            
            insert_price_record(conn)
            record_count += 1
            print(f"Total records: {record_count}")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        conn.close()

if __name__ == "__main__":
    main()