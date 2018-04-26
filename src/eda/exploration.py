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
    ''' This function takes in a time-series and a period (default to 20) and returns a new
        time-series with a historical volatility column, based on the period given.

        Args: ts - dataframe time-series of closing price data
              period - int number of days used to calculate the historical vol

        Return: cpy - dataframe new time-series with added hist vol column
    '''
    # Create a copy of the time-series to manipulate, and a string name for new column
    cpy = ts.copy()
    col_name = '{}d_hist_vol'.format(str(period))

    # Add historical vol column given period length
    cpy[col_name] = round(math.sqrt(252) * cpy.pct_change().rolling(window=period, center=False).std(), 4)

    return cpy

def plot_ts(ts):
    ''' This function takes in a time-series and plots the closing price and historical vol data.

        Args: ts - dataframe of time-series of closing price and historical vol data

        Return: None - plots both the closing price and historical vol data for time-series
    '''
    # Extract period from time-series column name
    period = ts.columns.tolist()[1][:ts.columns.tolist()[1].find('d')]

    # Create figure and axes to plot each series of data
    fig, (ax1, ax2) = plt.subplots(2,1, figsize=(12,8), gridspec_kw={'height_ratios': [3, 1]})

    # Plot the closing prices
    ax1.plot(ts.index, ts['close'])
    ax1.set(title='Time-series Closing Price and\n {}-day Historical Volatility'.format(period))
    ax1.set_ylabel('Closing Price')
    ax1.grid()

    # Plot the historical volatility
    ax2.plot(ts.index, ts['{}d_hist_vol'.format(period)], color='orange')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Volatility')
    ax2.grid()

    # Show both plots
    fig.tight_layout()
    fig.show()
