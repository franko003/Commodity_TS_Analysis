#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""" This module contains all the code for initial setup of the sqlite3 database to
    keep all data vendor, product, and price information.  It creates three separate
    tables and links them using table specific ids.
"""

import sqlite3

def db_setup(filename):
    # DATA VENDOR TABLE
    # Initialize variables for file name, table, columns, data types
    sqlite_file = filename
    table_name = 'Data_Vendor'
    id_col = 'id'
    name_col = 'name'
    url_col = 'url'
    dtype_int = 'INTEGER'
    dtype_text = 'TEXT'

    # Connect to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Create a new table with 3 columns
    c.execute('CREATE TABLE {tn} ({ic} {dti} PRIMARY KEY, {nc} {dtt}, {uc} {dtt})'\
         .format(tn=table_name, ic=id_col, dti=dtype_int, nc=name_col, dtt=dtype_text, uc=url_col))

    # Commit changes and close
    conn.commit()
    conn.close()

    # Add value for Quandl to Data_Vendor table
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute("INSERT INTO {tn} ({ic}, {nc}, {uc}) VALUES (1, 'Quandl', 'https://docs.quandl.com')"\
          .format(tn=table_name, ic=id_col, nc=name_col, uc=url_col))

    conn.commit()
    conn.close()

    # PRODUCTS TABLE
    # Create table for products
    # Initialize variables for file name, table, columns, data types
    table_name = 'Products'

    data_table = 'Data_Vendor'
    data_id = 'id'

    id_col = 'id'
    data_id_col = 'data_id'
    symbol_col = 'symbol'
    name_col = 'name'
    sector_col = 'sector'
    exchange_col = 'exchange'
    contracts_col = 'contracts'
    dtype_int = 'INTEGER'
    dtype_text = 'TEXT'

    # Connect to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Create a new table with 3 columns
    c.execute('CREATE TABLE {tn} ({ic} {dti} PRIMARY KEY,\
                              {dc} {dti},\
                              {sc} {dtt},\
                              {nc} {dtt},\
                              {sec} {dtt},\
                              {ec} {dtt},\
                              {cc} {dtt},\
                              FOREIGN KEY ({dc}) REFERENCES {dt} ({dic}))'\
         .format(tn=table_name, ic=id_col, dti=dtype_int, dc=data_id_col, sc=symbol_col,\
                 dtt=dtype_text, nc=name_col, sec=sector_col, ec=exchange_col,\
                 cc=contracts_col, dt=data_table, dic=data_id))

    # Commit changes and close
    conn.commit()
    conn.close()

    # CLOSING_PRICES TABLE
    # Create table for closing_price
    # Initialize variables for file name, table, columns, data types
    table_name = 'Closing_Prices'
    products_table = 'Products'

    id_col = 'id'
    data_id_col = 'data_id'
    symbol_col = 'symbol'
    date_col = 'date'
    open_col = 'open'
    high_col = 'high'
    low_col = 'low'
    close_col = 'close'
    volume_col = 'volume'

    dtype_int = 'INTEGER'
    dtype_text = 'TEXT'
    dtype_real = 'REAL'

    # Connect to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Create a new table with 3 columns
    c.execute('CREATE TABLE {tn} ({ic} {dti} PRIMARY KEY,\
                              {dc} {dti},\
                              {sc} {dtt},\
                              {dtc} {dtt},\
                              {oc} {dtr},\
                              {hc} {dtr},\
                              {lc} {dtr},\
                              {cc} {dtr},\
                              {vc} {dti},\
                              FOREIGN KEY ({sc}) REFERENCES {pt} ({sc}))'\
         .format(tn=table_name, ic=id_col, dti=dtype_int, dc=data_id_col, sc=symbol_col,\
                 dtt=dtype_text, dtc=date_col, oc=open_col, dtr=dtype_real, hc=high_col,\
                 lc=low_col, cc=close_col, vc=volume_col, pt=products_table))

    # Commit changes and close
    conn.commit()
    conn.close()
