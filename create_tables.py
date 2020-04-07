import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables in the current database as denoted by the drop table
    statements in sql_queries.py

    Parameters
    ----------
    cur, conn: the cursor and connection objects associated with the connected
        database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables in the current database as denoted by the create table
    statements in sql_queries.py

    Parameters
    ----------
    cur, conn: the cursor and connection objects associated with the connected
        database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Redshift cluster specified in the config file. Calls
    functions to drop all existing tables in the database, and then creates all
    tables. Closes the connection to the cluster upon completion.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
