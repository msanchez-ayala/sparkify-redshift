import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies data from JSON files in Udacity's S3 bucket to staging tables in
    Redshift.

    Parameters
    ----------
    cur, conn: the cursor and connection objects associated with the connected
        database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from the staging tables into the fact and dimension tables.

    Parameters
    ----------
    cur, conn: the cursor and connection objects associated with the connected
        database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Redshift cluster specified in the config file. Calls
    functions to load S3 data into staging tables and then inserts relevant data
    into the fact and dimension tables. Closes the connection to the cluster
    upon completion.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
