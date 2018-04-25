#!/usr/bin/env python
# # -*- coding: utf-8 -*-

""" This module contains functions to perform exploratory data analysis on a
    time-series of commodity futures closing price data.
"""

import pandas as pd
import numpy as np
import matplotlib.plyplot as plt
import seaborn as sns
import sqlite3
import math

def generate_time_series(product, start_year, end_year, db_file):
    ''' This function takes in a product, start_year, end_year, and database
        file then returns a dataframe of the time-series of closing prices given the parameters.

        Args: product - str product in database we want to create a time-series of closing prices
              start_year - int year to begin time-series
              end_year - int year to end the time-series
              db_file - str database filename

        Return: ts - the closing price time-series of product from start_year to end_year
    '''
    # Connect to database and create query
    conn = sqlite3.connect(db_file)
    query = "SELECT date, close FROM Closing_Prices\
             WHERE symbol = '{p}'\
                 AND date BETWEEN '{sy}-01-01' AND '{ey}-12-31';"\
            .format(p=product, sy=str(start_year), ey=str(end_year))

    # Read time-series data into dataframe
    ts = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])

    return ts

def add_hist_vol(ts, period=20):
    ''' This function takes in a time-series and a period (default to 20) and adds a historical
        volatility column to the time-series, based on the period given.

        Args: ts - dataframe time-series of closing price data
              period - int number of days used to calculate the historical vol

        Return: ts - dataframe time-series with added hist vol column
    '''
    # Add historical vol column given period length
    ts['{}d_hist_vol'.format(str(period))] = math.sqrt(252) * ts.pct_change().rolling(window=period, center=False).std()

    return ts
