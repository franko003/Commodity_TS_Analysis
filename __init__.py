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

if __name__ == "__main__":
    print('')
    print('This program runs a time-series analysis of specific commodity futures contracts')
    print('')
