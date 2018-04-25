#!/usr/bin/env python
# # -*- coding: utf-8 -*-

""" This is the module to run in order to setup the database.  Upon running this file,
    the data is collected, cleaned and stored into a SQL database file for each product in
    the product_map.
"""

from data.database import *

import os
from dotenv import load_dotenv, find_dotenv

# Load API_KEY from .env file
load_dotenv(find_dotenv())
API_KEY = os.getenv('API_KEY')

# Map of available products and information
product_map = {'CL': [1, 'Crude_Oil', 'Energy', 'CME', 'FGHJKMNQUVXZ'],
               'NG': [1, 'Natural_Gas', 'Energy', 'CME', 'FGHJKMNQUVXZ'],
               'HO': [1, 'Heating_Oil', 'Energy', 'CME', 'FGHJKMNQUVXZ'],
               'RB': [1, 'Gasoline', 'Energy', 'CME', 'FGHJKMNQUVXZ'],
               'B': [1, 'Brent_Crude_Oil', 'Energy', 'ICE', 'FGHJKMNQUVXZ'],
               'BO': [1, 'Soybean_Oil', 'Grains', 'CME', 'FHKNQUVZ'],
               'SM': [1, 'Soybean_Meal', 'Grains', 'CME', 'FHKNUVZ'],
               'W': [1, 'Wheat', 'Grains', 'CME', 'HKNUZ'],
               'C': [1, 'Corn', 'Grains', 'CME', 'HKNUZ'],
               'S': [1, 'Soybeans', 'Grains', 'CME', 'FHKNQUX'],
               'SB': [1, 'Sugar', 'Softs', 'ICE', 'HKNV'],
               'KC': [1, 'Coffee', 'Softs', 'ICE', 'HKNUZ'],
               'RC': [1, 'Robusta_Coffee', 'Softs', 'LIFFE', 'FHKNUX'],
               'CC': [1, 'Cocoa', 'Softs', 'ICE', 'HKNUZ'],
               'CT': [1, 'Cotton', 'Softs', 'ICE', 'HKNVZ']}

if __name__ == "__main__":
    print('')
    print('This program acquires, cleans, and stores data for all products')
    print('')

    db_file = input('Please provide a name for the database file (ex. my_db.sqlite): ')
    print('')

    print('.....Setting up database schema.....')
    db_setup(db_file)
    print('')

    print('.....Populating "Products" table.....')
    insert_products_table(product_map, db_file)
    print('')

    print('.....Creating a continuous time-series for each product.....')
    ts_dict = create_ts_dict(product_map, API_KEY)
    print('')

    print('.....Populating "Closing_Prices" table.....')
    insert_closing_prices_table(product_map, ts_dict, db_file)
    print('')

    print('.....Program complete.....')
