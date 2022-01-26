"""
Module for querying OSW files.
Features:
    - Query for tables

Goal: chunk data and stream it.
"""
import sqlite3

import pandas as pd


def get_osw_table_as_df(osw_path, tablename, chunksize):
    # OSW files can be opened with SQLite
    # Returns generator
    with sqlite3.connect(osw_path) as con:
        osw_table = pd.read_sql_query(f"SELECT * from {tablename}", con, chunksize=chunksize)
    return osw_table
