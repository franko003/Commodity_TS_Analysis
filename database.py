#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""" This module contains all the code for initial setup of the sqlite3 database to
    keep all data vendor, product, and price information.  It creates three separate
    tables and links them using table specific ids.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import quandl
from datetime import timedelta

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
    close_col = 'close'

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
                              {cc} {dtr},\
                              FOREIGN KEY ({sc}) REFERENCES {pt} ({sc}))'\
         .format(tn=table_name, ic=id_col, dti=dtype_int, dc=data_id_col, sc=symbol_col,\
                 dtt=dtype_text, dtc=date_col, cc=close_col, dtr=dtype_real, pt=products_table))

    # Commit changes and close
    conn.commit()
    conn.close()

def insert_products_table(product_map, sqlite_file, table_name='Products'):
    ''' This function takes in a dict of product symbols mapped to
        information about the product.  It also takes in a sqlite file and then
        uses the info to insert all symbols in the dict into the Products
        table of the database.

        Args: product_dict - a dict of symbols for products with maps to
                             a list of info
              sqlite_file - file for the database to write to
              table_name - default to 'Products' for this function

        Return: None - nothing explicit but inserts info into the database
    '''
    # Create the column name list for database insertion
    cols = ['data_id', 'symbol', 'name', 'sector', 'exchange', 'contracts']

    # Open a connection to the database
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Iterate through all symbols of product_dict
    for symbol, s_info in product_map.items():
        # Set params and insert row into database
        params = (s_info[0], symbol, s_info[1], s_info[2], s_info[3], s_info[4])
        c.execute("INSERT INTO {tn} ({c0}, {c1}, {c2}, {c3}, {c4}, {c5}) VALUES (?, ?, ?, ?, ?, ?)"\
            .format(tn=table_name, c0=cols[0], c1=cols[1], c2=cols[2],\
            c3=cols[3], c4=cols[4], c5=cols[5]), params)

    # Close connection to database
    conn.commit()
    conn.close()

def insert_closing_prices_table(product_map, ts_dict, sqlite_file, table_name='Closing_Prices'):
    ''' This function takes in a 2 dicts, one with product keys mapping
        to info about the product and the other with product keys mapping
        to a time-series of closing price information.  It also takes in a sqlite
        file and then uses the info to insert all rows into the Closing_Prices
        table of the database.

        Args: product_dict - a dict of symbols for products with maps to
                             a list of info
              df_dict - dict of dataframes with futures symbols and price data
              sqlite_file - file for the database to write to
              table_name - default to 'Closing_Prices' for this function

        Return: None - nothing explicit but inserts info into the database
    '''
    # Create the column name list for database insertion
    cols = ['data_id', 'symbol', 'date', 'close']

    # Open a connection to the database
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Iterate through all symbols and then the dataframe to get all price data
    for symbol, ts in ts_dict.items():
        data_id = product_map[symbol][0]
        for i, row in ts.iterrows():
            date = i.strftime('%Y-%m-%d')
            # Set params and insert row into database
            params = (data_id, symbol, date, row.close)
            c.execute("INSERT INTO {tn} ({c0}, {c1}, {c2}, {c3}) VALUES (?, ?, ?, ?)"\
                .format(tn=table_name, c0=cols[0], c1=cols[1], c2=cols[2], c3=cols[3]), params)

    # Close connection to database
    conn.commit()
    conn.close()
