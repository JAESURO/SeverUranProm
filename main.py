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
                'name': '1. Nuclear Power Plants',
                'query': 'SELECT * FROM powerplants LIMIT 10',
                'params': None
            },
            {
                'name': '2. Uranium Companies',
                'query': 'SELECT * FROM uranium_companies LIMIT 10',
                'params': None
            },
            {
                'name': '3. Uranium Mines',
                'query': 'SELECT * FROM uranium_mines LIMIT 10',
                'params': None
            },
            {
                'name': '4. High-Capacity Nuclear Plants (by MW)',
                'query': 'SELECT name_of_powerplant, capacity_mw FROM powerplants WHERE capacity_mw > 2000 ORDER BY capacity_mw DESC LIMIT 10',
                'params': None
            },
            {
                'name': '5. Top Uranium Producers (by tonnes)',
                'query': 'SELECT company, tonnes_u FROM uranium_companies WHERE tonnes_u > 5000 ORDER BY tonnes_u DESC LIMIT 10',
                'params': None
            },
            {
                'name': '6. "Nuclear" Power Plants by Country',
                'query': 'SELECT name_of_powerplant, capacity_mw FROM powerplants WHERE primary_fuel = \'Nuclear\' ORDER BY capacity_mw DESC LIMIT 10',
                'params': None
            },
            {
                'name': '7. Nuclear Plants Count by Country',
                'query': 'SELECT country_long, COUNT(*) FROM powerplants GROUP BY country_long LIMIT 10',
                'params': None
            },
            {
                'name': '8. Uranium Production by Country',
                'query': 'SELECT country_long, SUM(tonnes_u) FROM uranium_companies GROUP BY country_long ORDER BY SUM(tonnes_u) DESC LIMIT 10',
                'params': None
            },
            {
                'name': '9. Uranium Mines with Companies',
                'query': 'SELECT m.mine_name, m.country_long, c.company FROM uranium_mines m JOIN uranium_companies c ON m.country_long = c.country_long LIMIT 10',
                'params': None
            },
            {
                'name': '10. Uranium Mines and Reserves by Country',
                'query': 'SELECT m.country_long, COUNT(*) as mine_count, r.tonnes_uranium FROM uranium_mines m JOIN uranium_reserves r ON m.country_long = r.country_long GROUP BY m.country_long, r.tonnes_uranium ORDER BY r.tonnes_uranium DESC LIMIT 10',
                'params': None
            }
        ]
        
        for i, query_info in enumerate(queries, 1):
            print(f"{query_info['name']}")
            
            try:
                cur.execute(query_info['query'])
                rows = cur.fetchall()
                
                if rows:
                    for idx, row in enumerate(rows, 1):
                        print(f"{idx}. {row}")
                else:
                    print("No results found")
                    
            except Exception as e:
                print(f"Error: {e}")

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    run_queries()