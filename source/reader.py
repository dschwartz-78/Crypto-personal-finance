#!/usr/bin/python

# Usage
# source cpf-env/bin/activate
# python3

# Imports
import os
import glob
import csv
import pandas as pd
import pprint


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

        nEntries = len(data) - 1
        nColomns = len(data[0])

        # Convert to Panda data frame
        index = pd.to_datetime(index, format='%Y-%m-%dT%H:%M')      # convert to pandas timestamp format
        # print(index)
        # pprint.pprint(data)
        df = pd.DataFrame(data, index=index, columns=header)

    return df

def read_coinbase_history():
    # Change directory form /source to /input/coinbase
    path = os.getcwd()                  # get current directory
    path = os.path.dirname(path)        # move one folder up
    path = path + '/input/coinbase/'    # move to CoinBase folder

    # Read every csv file in the /input/coinbase folder
    for file in glob.glob(path + '*.csv'):
        print("Read CoinBase file : " + file)
        df = read_coinbase_file(file)
        print(df)

read_coinbase_history()
