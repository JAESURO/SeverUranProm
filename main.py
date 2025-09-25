import psycopg2
from config import config


def get_public_tables(cursor):
    cursor.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'public' and table_type = 'BASE TABLE'
        order by table_name
        """
    )
    return [row[0] for row in cursor.fetchall()]


def get_columns(cursor, table_name):
    cursor.execute(
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'public' and table_name = %s
        order by ordinal_position
        """,
        (table_name,),
    )
    return cursor.fetchall()


def get_foreign_key_joins(cursor):
    cursor.execute(
        """
        SELECT
            tc.table_name       AS source_table,
            kcu.column_name     AS source_column,
            ccu.table_name      AS target_table,
            ccu.column_name     AS target_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        ORDER BY source_table, target_table
        """
    )
    return cursor.fetchall()


def pick_first_numeric_column(columns):
    numeric_types = {
        'smallint', 'integer', 'bigint', 'real', 'double precision',
        'numeric', 'decimal'
    }
    for name, dtype in columns:
        if dtype in numeric_types:
            return name
    return None


def pick_first_text_column(columns):
    text_types = {'text', 'varchar', 'character varying', 'character', 'citext'}
    for name, dtype in columns:
        if dtype in text_types:
            return name
    return None


def run_queries():
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        cur.execute('select version()')
        print(cur.fetchone()[0])

        tables = get_public_tables(cur)
        if not tables:
            print('No user tables found in schema public. Exiting.')
            return

        def print_rows_from_cursor():
            cols = [d[0] for d in cur.description]
            print('\t'.join(cols))
            for r in cur.fetchall():
                print('\t'.join('' if v is None else str(v) for v in r))

        for sample_table in tables[:3]:
            try:
                print(f"\nSAMPLE {sample_table}")
                cur.execute(f'SELECT * FROM "{sample_table}" LIMIT 5')
                print_rows_from_cursor()
            except Exception:
                pass

        queries = [
            {
                'name': 'Basic SELECT from powerplants table (longtitude and latitude not included)',
                'query': '''
                    SELECT country_code, country_long, name_of_powerplant, capacity_mw, 
                           primary_fuel, secondary_fuel, start_date, owner_of_plant, 
                           generation_gwh_2021, geolocation_source, estimated_generation_gwh_2021
                    FROM powerplants
                    LIMIT 10
                ''',
                'params': None
            },
            {
                'name': 'Filtering and sorting uranium companies by tones less than 1000',
                'query': '''
                    SELECT company, tonnes_u, percentage, country_long
                    FROM uranium_companies
                    WHERE tonnes_u > 1000
                    ORDER BY tonnes_u DESC
                    LIMIT 10
                ''',
                'params': None
            },
            {
                'name': 'Aggregation by tonnes of uranium produced',
                'query': '''
                    SELECT 
                        CASE 
                            WHEN tonnes_u < 5000 THEN 'Small Producer (< 5k tonnes)'
                            WHEN tonnes_u < 15000 THEN 'Medium Producer (5k-15k tonnes)'
                            ELSE 'Large Producer (> 15k tonnes)'
                        END as production_category,
                        COUNT(*) as company_count,
                        AVG(tonnes_u) as avg_tonnes,
                        MIN(tonnes_u) as min_tonnes,
                        MAX(tonnes_u) as max_tonnes
                    FROM uranium_companies
                    WHERE tonnes_u IS NOT NULL
                    GROUP BY 
                        CASE 
                            WHEN tonnes_u < 5000 THEN 'Small Producer (< 5k tonnes)'
                            WHEN tonnes_u < 15000 THEN 'Medium Producer (5k-15k tonnes)'
                            ELSE 'Large Producer (> 15k tonnes)'
                        END
                    ORDER BY avg_tonnes DESC
                    LIMIT 10
                ''',
                'params': None
            },
            {
                'name': 'JOIN between uranium companies and powerplants',
                'query': '''
                    SELECT 
                        c.company,
                        c.tonnes_u,
                        c.country_long,
                        COUNT(p.name_of_powerplant) as powerplant_count,
                        SUM(p.capacity_mw) as total_capacity_mw
                    FROM uranium_companies c
                    JOIN powerplants p ON c.country_long = p.country_long
                    WHERE c.tonnes_u IS NOT NULL
                    GROUP BY c.company, c.tonnes_u, c.country_long
                    ORDER BY c.tonnes_u DESC
                    LIMIT 10
                ''',
                'params': None
            }
        ]
        
        for i, query_info in enumerate(queries, 1):
            print(f"\n{'='*60}")
            print(f"Query {i}: {query_info['name']}")
            print('='*60)
            
            try:
                if query_info['params']:
                    cur.execute(query_info['query'], query_info['params'])
                else:
                    cur.execute(query_info['query'])
                
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                
                if rows:
                    print(f"{'№':<3} | " + " | ".join(f"{col:<15}" for col in cols))
                    print("-" * (7 + len(cols) * 18))
                    
                    for idx, row in enumerate(rows, 1):
                        formatted_row = []
                        for val in row:
                            if val is None:
                                formatted_row.append("NULL")
                            elif isinstance(val, (int, float)):
                                formatted_row.append(str(val))
                            else:
                                str_val = str(val)
                                formatted_row.append(str_val[:15] + "..." if len(str_val) > 15 else str_val)
                        
                        print(f"{idx:<3} | " + " | ".join(f"{val:<15}" for val in formatted_row))
                else:
                    print("No results found")
                    
            except Exception as e:
                print(f"Error executing query: {e}")

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    run_queries()