import time, random, psycopg2, sys, os
from datetime import datetime
import logging
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def generate_load(duration_hours=3):
    logger.info(f"Starting database load generation for {duration_hours} hours...")
    try:
        conn = psycopg2.connect(**config())
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        end_time = time.time() + (duration_hours * 3600)
        counts = {'query': 0, 'insert': 0, 'update': 0, 'delete': 0, 'transaction': 0}
        select_queries = [
            "SELECT COUNT(*) FROM powerplants", "SELECT COUNT(*) FROM mines",
            "SELECT COUNT(*) FROM companies", "SELECT COUNT(*) FROM reserves",
            "SELECT COUNT(*) FROM production", "SELECT COUNT(*) FROM prices",
            "SELECT * FROM powerplants LIMIT 10", "SELECT * FROM mines LIMIT 5",
            "SELECT * FROM companies LIMIT 5",
            "SELECT country_long, COUNT(*) FROM powerplants GROUP BY country_long LIMIT 10",
            "SELECT pg_database_size(current_database())",
            "SELECT count(*) FROM pg_stat_activity", "SELECT COUNT(*) FROM information_schema.tables"
        ]
        
        while time.time() < end_time:
            try:
                op = random.choice(['select', 'select', 'select', 'insert', 'update', 'delete', 'transaction'])
                
                if op == 'select':
                    cursor.execute(random.choice(select_queries))
                    cursor.fetchall()
                    counts['query'] += 1
                
                elif op == 'insert':
                    table = random.choice(['prices', 'prices', 'prices', 'companies', 'mines', 'reserves'])
                    try:
                        if table == 'prices':
                            cursor.execute("INSERT INTO prices (year_, month_, price_uranium, inflation_rate, price_uran_inflation) VALUES (%s, %s, %s, %s, %s)",
                                         (datetime.now().year, random.choice(MONTHS), round(random.uniform(35, 55), 2),
                                          round(random.uniform(2, 8), 2), round(random.uniform(35, 55) * (1 + random.uniform(2, 8)/100), 6)))
                        elif table == 'companies':
                            cursor.execute("INSERT INTO companies (company, tonnes_uranium, percent_) VALUES (%s, %s, %s)",
                                         (f"Test Company {random.randint(1000, 9999)}", round(random.uniform(100, 10000), 2), round(random.uniform(0.1, 50), 2)))
                        elif table == 'mines':
                            cursor.execute("INSERT INTO mines (mine, country_long, main_owner, type, production_tonnes_uranium, percent_of_world) VALUES (%s, %s, %s, %s, %s, %s)",
                                         (f"Test Mine {random.randint(1000, 9999)}", random.choice(['Kazakhstan', 'Canada', 'Australia', 'Niger', 'Russia', 'Namibia']),
                                          f"Test Owner {random.randint(1, 100)}", random.choice(['ISL', 'Open Pit', 'Underground']),
                                          round(random.uniform(100, 5000), 2), round(random.uniform(0.1, 20), 2)))
                        elif table == 'reserves':
                            cursor.execute("INSERT INTO reserves (country_long, tonnes_of_uranium, percentage_of_world) VALUES (%s, %s, %s)",
                                         (random.choice(['Kazakhstan', 'Canada', 'Australia', 'Niger', 'Russia', 'Namibia', 'Uzbekistan', 'China']),
                                          round(random.uniform(1000, 100000), 2), round(random.uniform(0.1, 30), 2)))
                        conn.commit()
                        counts['insert'] += 1
                        counts['query'] += 1
                    except Exception as e:
                        conn.rollback()
                        logger.debug(f"Insert failed: {e}")
                
                elif op == 'update':
                    table = random.choice(['prices', 'companies', 'mines', 'reserves', 'production'])
                    try:
                        cursor.execute(f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 1")
                        result = cursor.fetchone()
                        if result:
                            if table == 'prices':
                                cursor.execute("UPDATE prices SET price_uranium = %s WHERE id = %s", (round(random.uniform(35, 55), 2), result[0]))
                            elif table == 'companies':
                                cursor.execute("UPDATE companies SET tonnes_uranium = %s WHERE id = %s", (round(random.uniform(100, 10000), 2), result[0]))
                            elif table == 'mines':
                                cursor.execute("UPDATE mines SET production_tonnes_uranium = %s WHERE id = %s", (round(random.uniform(100, 5000), 2), result[0]))
                            elif table == 'reserves':
                                cursor.execute("UPDATE reserves SET tonnes_of_uranium = %s WHERE id = %s", (round(random.uniform(1000, 100000), 2), result[0]))
                            elif table == 'production':
                                cursor.execute(f'UPDATE production SET "{random.choice(["2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014"])}" = %s WHERE id = %s',
                                             (round(random.uniform(100, 10000), 2), result[0]))
                            conn.commit()
                            counts['update'] += 1
                            counts['query'] += 1
                    except Exception as e:
                        conn.rollback()
                        logger.debug(f"Update failed: {e}")
                
                elif op == 'delete':
                    table = random.choice(['prices', 'companies', 'mines', 'reserves'])
                    try:
                        where = "WHERE company LIKE 'Test Company%'" if table == 'companies' else "WHERE mine LIKE 'Test Mine%'" if table == 'mines' else ""
                        cursor.execute(f"SELECT id FROM {table} {where} ORDER BY RANDOM() LIMIT 1")
                        result = cursor.fetchone()
                        if result:
                            cursor.execute(f"DELETE FROM {table} WHERE id = %s", (result[0],))
                            conn.commit()
                            counts['delete'] += 1
                            counts['query'] += 1
                    except Exception as e:
                        conn.rollback()
                        logger.debug(f"Delete failed: {e}")
                
                elif op == 'transaction':
                    try:
                        if random.random() < 0.7:
                            cursor.execute("SELECT COUNT(*) FROM powerplants")
                            cursor.fetchone()
                            cursor.execute("SELECT COUNT(*) FROM mines")
                            cursor.fetchone()
                            conn.commit()
                        else:
                            cursor.execute("SELECT COUNT(*) FROM companies")
                            cursor.fetchone()
                            conn.rollback()
                        counts['transaction'] += 1
                        counts['query'] += 1
                    except Exception as e:
                        logger.debug(f"Transaction failed: {e}")
                
                if counts['query'] % 50 == 0:
                    logger.info(f"Executed {counts['query']} queries (I:{counts['insert']}, U:{counts['update']}, D:{counts['delete']}, T:{counts['transaction']}), {duration_hours - (end_time - time.time())/3600:.1f}h remaining")
                
                time.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(1)
        
        cursor.close()
        conn.close()
        logger.info(f"Load generation completed. Total queries: {counts['query']} (I:{counts['insert']}, U:{counts['update']}, D:{counts['delete']}, T:{counts['transaction']})")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")

if __name__ == '__main__':
    generate_load(duration_hours=3)