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

        table_a = tables[0]
        columns_a = get_columns(cur, table_a)
        first_col_a = columns_a[0][0]

        # 1) SELECT * FROM table LIMIT 10
        print(f"\nQ1 SELECT * FROM {table_a} LIMIT 10")
        cur.execute(f'SELECT * FROM "{table_a}" LIMIT 10')
        print_rows_from_cursor()

        # 2) WHERE filter and ORDER BY
        text_col = pick_first_text_column(columns_a)
        if text_col:
            print(f"\nQ2 WHERE + ORDER BY on {table_a}")
            cur.execute(
                f"SELECT * FROM \"{table_a}\" WHERE \"{text_col}\" ILIKE %s ORDER BY \"{first_col_a}\" DESC LIMIT 10",
                ('%a%',),
            )
        else:
            print(f"\nQ2 WHERE + ORDER BY on {table_a}")
            cur.execute(
                f"SELECT * FROM \"{table_a}\" WHERE \"{first_col_a}\" IS NOT NULL ORDER BY \"{first_col_a}\" DESC LIMIT 10"
            )
        print_rows_from_cursor()

        # 3) Aggregation with GROUP BY (count, avg, min, max)
        numeric_col = pick_first_numeric_column(columns_a)
        group_by_col = text_col or first_col_a
        if numeric_col:
            print(f"\nQ3 GROUP BY on {table_a}")
            cur.execute(
                f"""
                SELECT \"{group_by_col}\" AS group_key,
                       COUNT(*) AS cnt,
                       AVG(\"{numeric_col}\") AS avg_val,
                       MIN(\"{numeric_col}\") AS min_val,
                       MAX(\"{numeric_col}\") AS max_val
                FROM \"{table_a}\"
                GROUP BY \"{group_by_col}\"
                ORDER BY cnt DESC NULLS LAST
                LIMIT 10
                """
            )
        else:
            agg_source = text_col or first_col_a
            print(f"\nQ3 GROUP BY on {table_a}")
            cur.execute(
                f"""
                SELECT \"{group_by_col}\" AS group_key,
                       COUNT(*) AS cnt,
                       AVG(LENGTH(COALESCE(\"{agg_source}\"::text, ''))) AS avg_len,
                       MIN(\"{agg_source}\"::text) AS min_val,
                       MAX(\"{agg_source}\"::text) AS max_val
                FROM \"{table_a}\"
                GROUP BY \"{group_by_col}\"
                ORDER BY cnt DESC NULLS LAST
                LIMIT 10
                """
            )
        print_rows_from_cursor()

        # 4) JOIN between tables (prefer actual foreign keys)
        if len(tables) > 1:
            fk_joins = get_foreign_key_joins(cur)
            if fk_joins:
                src_table, src_col, tgt_table, tgt_col = fk_joins[0]
                print(f"\nQ4 JOIN {src_table}.{src_col} -> {tgt_table}.{tgt_col}")
                cur.execute(
                    f"""
                    SELECT a.*, b.*
                    FROM \"{src_table}\" a
                    JOIN \"{tgt_table}\" b ON a.\"{src_col}\" = b.\"{tgt_col}\"
                    LIMIT 10
                    """
                )
                print_rows_from_cursor()
            else:
                table_b = tables[1]
                columns_b = get_columns(cur, table_b)
                cols_a_set = {c[0] for c in columns_a}
                cols_b_set = {c[0] for c in columns_b}
                common_cols = list(cols_a_set.intersection(cols_b_set))

                if common_cols:
                    join_col = common_cols[0]
                    print(f"\nQ4 JOIN {table_a}.{join_col} = {table_b}.{join_col}")
                    cur.execute(
                        f"""
                        SELECT a.*, b.*
                        FROM \"{table_a}\" a
                        JOIN \"{table_b}\" b ON a.\"{join_col}\" = b.\"{join_col}\"
                        LIMIT 10
                        """
                    )
                else:
                    print(f"\nQ4 CROSS JOIN {table_a} x {table_b}")
                    cur.execute(
                        f"SELECT a.*, b.* FROM \"{table_a}\" a CROSS JOIN \"{table_b}\" b LIMIT 10"
                    )
                print_rows_from_cursor()
        else:
            pass

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    run_queries()