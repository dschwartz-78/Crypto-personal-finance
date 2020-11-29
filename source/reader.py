#!/usr/bin/python

# Usage
# source cpf-env/bin/activate
# python3

# Imports
import os
import glob
import csv
import numpy as np
import pandas as pd
import pprint


# Parameters

coinbase_column_actions = {
    # Rename existing columns
  "Transaction Type": {
      "action" : "rename",
      "value" : "Transaction Type",
      "order" : 1,
  },
  "Asset": {
      "action" : "rename",
      "value" : "Destination currency",
      "order" : 2,
  },
  "Quantity Transacted": {
      "action" : "rename",
      "value" : "Destination amount",
  },
  "EUR Spot Price at Transaction": {
      "action" : "rename",
      "value" : "Exchange rate",
  },
  "EUR Subtotal": {
      "action" : "rename",
      "value" : "EUR Subtotal",
  },
  "EUR Total (inclusive of fees)": {
      "action" : "rename",
      "value" : "Equivalent value EUR",
  },
  "EUR Fees": {
      "action" : "rename",
      "value" : "Fee amount EUR",
  },
  "Notes": {
      "action" : "rename",
      "value" : "Notes",
  },
  # Add new columns
  "Origin platform": {
      "action" : "create",
  },
  "Origin currency": {
      "action" : "create",
  },
  "Destination platform": {
      "action" : "create",
  },
  "Transaction ID": {
      "action" : "create",
  },
  "Fee amount": {
      "action" : "create",
  },
  "Fee curreny": {
      "action" : "create",
  },
  # Drop columns
}

column_order = [
    "Origin platform",
    "Origin amount",
    "Origin currency",
    "Destination platform",
    "Destination amount",
    "Destination currency",
    "Transaction type",
    "Transaction rate",
    "Transaction ID",
    "Fee amount",
    "Fee currency",
    "Fee EUR",
    "Equivalent value EUR",
]


# Helper functions for readinf user parameters

# CoinBase
def rename_coinbase_column():
    coinbase_column_rename = {}
    for column, attr in coinbase_column_actions.items():
        if attr['action'] == 'rename':
            coinbase_column_rename[column] = attr['value']
    # coinbase_column_rename = {
    #   "Transaction Type": "Transaction type",
    #   "Asset": "Destination currency",
    #   "Quantity Transacted": "Destination amount",
    #   "EUR Spot Price at Transaction": "Exchange rate",
    #   "EUR Subtotal": "EUR Subtotal",
    #   "EUR Total (inclusive of fees)": "Equivalent value EUR",
    #   "EUR Fees": "Fee amount EUR",
    #   "Notes": "Notes",
    # }
    return coinbase_column_rename

def create_coinbase_column():
    coinbase_column_create = []
    for column, attr in coinbase_column_actions.items():
        if attr['action'] == 'create':
            coinbase_column_create.append(column)
        # coinbase_column_create = [
        #     "Origin platform",
        #     "Origin currency",
        #     "Destination platform",
        #     "Transaction ID",
        #     "Fee amount",
        #     "Fee curreny",
        # ]
    return coinbase_column_create

def read_coinbase_file(csvFile):
    df = None
    with open(csvFile, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        storeData = False
        data = []
        index = []
        for row in csv_reader:
            if row:                         # checks if row is empty
                if storeData:
                    data.append(row[1:])    # store data
                    index.append(row[0])    # store transction time
                if row[0]=='Timestamp':     # checks if row contains 'Timestamp'
                    storeData = True        # authorize data storage
                    header = row[1:]        # store the columns labels

        # Convert data to Pandas data frame
        index = pd.to_datetime(index, format='%Y-%m-%dT%H:%M')      # convert to pandas timestamp format
        df = pd.DataFrame(data, index=index, columns=header)        # convert data to pandas data frame

    return df


# Binance
def rename_binance_column():
    binance_column_rename = {}
    for column, attr in binance_column_actions.items():
        if attr['action'] == 'rename':
            binance_column_rename[column] = attr['value']
    # binance_column_rename = {
    #   "Amount": "Origin amount",
    #   "Market": "Origin currency",
    #   "Type": "Transaction type",
    #   "Price": "Transaction rate",
    #   "Total": "Destination amount",
    #   "Fee": "Fee amount",
    #   "Fee Coin": "Fee currency",
    #   "Notes": "Notes",
    # }
    return binance_column_rename

def create_binance_column():
    binance_column_create = []
    for column, attr in binance_column_actions.items():
        if attr['action'] == 'create':
            binance_column_create.append(column)
    # binance_column_create = [
    #     "Origin platform",
    #     "Destination platform",
    #     'Destination currency',
    #     "Transaction ID",
    #     "Fee amount EUR",
    #     "Equivalent value EUR",
    # ]

    return binance_column_create

def read_binance_file(excelFile):
    df = pd.read_excel(excelFile)               # read Excel file
    print('1 *****')
    print(df)
    df.index = df[df.columns.values[0]]         # set first column as index
    print('2 *****')
    print(df)
    temporalDataLabel = df.columns.values[0]    # get first column name (containing temporal data)
    print('3 *****')
    print(df)
    df.drop(columns=['Date(UTC)', 'Market'])          # drop first column (as it has been set as index)
    print('4 *****')
    print(df)
    return df


# Standardise Data Frame

def standardise_coinbase_df(df):
    print(df)
    for i in np.arange(len(df.columns.values)):
        label = df.columns.values[i]
        if coinbase_column_actions[label]["action"] == "rename":
            df.columns.values[i] = coinbase_column_actions[label]["value"]
        # df.columns.values[column] = coinbase_column_mapping(column)
    print(df)
    return df

def standardise_binance_df(df):
    print(df)
    for i in np.arange(len(df.columns.values)):
        label = df.columns.values[i]
        if coinbase_column_actions[label]["action"] == "rename":
            df.columns.values[i] = coinbase_column_actions[label]["value"]
        # df.columns.values[column] = coinbase_column_mapping(column)
    print(df)
    return df


# File readers

def read_coinbase_history():
    # Change directory form /source to /input/coinbase
    path = os.getcwd()                  # get current directory
    path = os.path.dirname(path)        # move one folder up
    path = path + '/input/coinbase/'    # move to CoinBase folder

    # Read every csv file in the /input/coinbase folder
    df_list = []
    nFiles = 0
    for file in glob.glob(path + '*.csv'):
        nFiles += 1
        print("Read CoinBase files")
        print("  file #" + str(nFiles))
        print("    Path : " + file)
        df = read_coinbase_file(file)
        print("    Convert to Data Frame")
        df = standardise_coinbase_df(df)
        df_list.append(df)
    print("  Total " + str(nFiles) + " CoinBase files")
    return df_list

def read_binance_history():
    # Change directory form /source to /input/binance
    path = os.getcwd()                  # get current directory
    path = os.path.dirname(path)        # move one folder up
    path = path + '/input/binance/'     # move to Binance folder

    # Read every xlsx file in the /input/binance folder
    nFiles = 0
    df_list = []
    for file in glob.glob(path + '*.xlsx'):
        nFiles += 1
        print("Read Binance files")
        print("  file #" + str(nFiles))
        print("    Path : " + file)
        df = read_binance_file(file)
        print("    Convert to Data Frame")
        # df = standardise_binance_df(df)
        # print(df)
        df_list.append(df)
    print("  Total " + str(nFiles) + " Binance files")
    return df_list


def read_history():
    # coinbase_history = read_coinbase_history()
    binance_history = read_binance_history()

read_history()
