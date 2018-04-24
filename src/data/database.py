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

def clean_df(df, days=200):
    ''' This function takes in a dataframe and number of days and returns a cleaned dataframe, with only the last
        "days" number of data points, with irrelevant data dropped and relevant columns renamed, and the `change`
        column corrected.

        Args: df - dataframe of price information
              days - number of data points to extract from the end

        Return: df - dataframe cleaned, with relevant and correct price information
    '''
    # Filter data to get the last 200 data points with a real settle price
    df = df[df.Settle > 0.0][-days:]

    # Drop unnecessary columns and rename
    df = df.reindex(columns=['Open', 'High', 'Low', 'Settle', 'Volume', 'Open Interest'])
    df.rename(columns={'Open': 'open',
                       'High': 'high',
                       'Low': 'low',
                       'Settle': 'close',
                       'Volume': 'volume',
                       'Open Interest': 'open_interest'}, inplace=True)

    # Add `change` back in correctly
    df['change'] = df.close - df.close.shift()

    return df[1:]

def get_quandl_list(product, product_map, api_key, start_year=2006, end_year=2016):
    ''' This function takes in a product, map, start and end year as well as an api key for quandl,
        and returns a list of dataframes for price information on each futures contract from the start
        date to the end date.

        Args: product - str symbol for specific product
              product_map - dict mapping product symbols to information
              start_year - int year to start analysis
              end_year - int year to end analysis
              api_key - str api key for quandl

        Return: df_list - list of dataframes of price information
    '''
    # Initialize final list of dataframes
    df_list = []

    # Set exchange and contract information for product
    exch = product_map[product][3]
    contracts = product_map[product][4]

    # Iterate through all years
    for year in range(start_year, end_year + 1):
        # Over all contract months
        for month in contracts:
            # Call quandl API, clean data and append to final list
            df = quandl.get('{}/{}{}{}'.format(exch, product, month, year), authtoken=api_key)
            df = clean_df(df)
            df_list.append(df)

    return df_list

def concat_contracts(df1, df2):
    ''' This function takes in 2 dataframes of closing price information for consecutive futures
        contracts and concatenates them using a 4-day rolling window.  It returns a dataframe of the
        concatenation.

        Args: df1 - the first dataframe of closing price info
              df2 - the next dataframe of closing price info

        Return: concat_df - dataframe of the concatenation of df1 and df2
    '''
    # Store important roll dates for indexing later
    last_date = df1.index[-5]
    roll_dates = [df1.index[-x] for x in range(4,0,-1)]
    df1_roll_index = df1.index.get_loc(roll_dates[0])
    df2_roll_index = df2.index.get_loc(roll_dates[0])
    first_date = df2.index[df2_roll_index + 4]

    # Create a list of the roll calculations for each day
    roll_calc = [((0.8 * df1.close[df1_roll_index]) + 0.2 * df2.close[df2_roll_index]),
                 ((0.6 * df1.close[(df1_roll_index + 1)]) + 0.4 * df2.close[(df2_roll_index + 1)]),
                 ((0.4 * df1.close[(df1_roll_index + 2)]) + 0.6 * df2.close[(df2_roll_index + 2)]),
                 ((0.2 * df1.close[(df1_roll_index + 3)]) + 0.8 * df2.close[(df2_roll_index + 3)])]

    # Take all data from first series up to last_date
    concat_df = pd.DataFrame(df1.close[:last_date])
    concat_df.columns = ['close']

    # Iterate through roll_dates and roll_calc to append roll data
    for i in range(4):
        concat_df.loc[roll_dates[i]] = roll_calc[i]

    # Finally concatenate the rest of the data from the second series
    concat_df = pd.DataFrame(pd.concat([concat_df.close, df2.close[first_date:]]))
    concat_df.columns = ['close']

    return concat_df

def combine_list(df_list):
    ''' This function takes in a list of dataframes and combines them, returning a final_df which is the
        concatenation of all dataframes in the list.

        Args: df_list - list of dataframes in consecutive order

        Return: final_df - dataframe which is the concatenation of the list of dataframes
    '''
    # If only one dataframe in list just return it
    if len(df_list) == 1:
        return df_list[0]

    # Set final dataframe equal to first in list
    final_df = df_list[0]

    # Iterate through the rest of the dataframes and apply `concat_contracts` function
    for i in range(1, len(df_list)):
        final_df = concat_contracts(final_df, df_list[i])

    return final_df

def create_ts_dict(product_map, api_key):
    ''' This function uses the helper functions to create a ts_dict based on all products in
        the product_map.  It returns a time-series dict of symbol mapped to time-series.

        Args: product_map - dict of symbols to price information
              api_key - str api_key for quandl

        Return: ts_dict - dict of symbol mapped to time-series
    '''
    # Initialize dict for time-series
    ts_dict = {}

    # Iterate through product_map and populate the ts_dict
    for product in product_map:
        prod_list = get_quandl_list(product, product_map, api_key)
        prod_df = combine_list(prod_list)
        ts_dict[product] = prod_df

    return ts_dict
