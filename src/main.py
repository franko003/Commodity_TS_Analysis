#!/usr/bin/env python
# # -*- coding: utf-8 -*-

""" This is the starting point for the entire program.  Upon running this file, the
    entire workflow is completed, from acquiring, cleaning, and storing the data,
    to exploring and then running inferential and time-series analysis.
"""

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
    print('This program runs a time-series analysis of specific commodity futures contracts')
    print('')
