import pandas as pd
import yaml
import numpy


class Processor:
    """Class to process the history DataFrame"""

    # Create attributes (to be filled during computation)
    platforms = numpy.ndarray
    preferences = {}
    df = pd.DataFrame()
    fee = {}
    gain = {}
    eq_perf = {}
    perf_topic = {}

    # Static variables
    topics = {
        'payment': ['BTC', 'ETH', 'XRP', 'BCH', 'LTC', 'LINK', 'EOS', 'XLM', 'ADA',
                    'XMR', 'TRX', 'DASH', 'ETC', 'ZEC', 'BAT', 'ALGO', 'ICX', 'WAVES',
                    'OMG', 'GNO', 'MLN', 'NANO', 'DOGE', 'USDT', 'DAI', 'SC', 'LSK',
                    'XTZ', 'ATOM', 'REP'],
        'infrastructure': ['ETH', 'ETC', 'ICX', 'EOS', 'OMG', 'ADA', 'TRX', 'ALGO',
                           'WAVES', 'LSK', 'XTZ', 'ATOM'],
        'financial': ['GNO', 'MLN', 'REP', 'COMP', 'LEND'],
        'service': ['LINK', 'SC', 'STORJ'],
        'entertainment': ['BAT'],
    }


    def load_preferences():
        """Load values from preferences file input/preferences.yaml"""

        # Load default values
        preferences = {
            'indicators': [
                {'salary': 2000},
                {'bonnat': 5}
            ],
            'output': [
                {'skin': 'skin-2020'}
            ],
        }

        # Read YAML preferences file
        pref_file = '../input/preferences.yaml'
        with open(pref_file) as file:
            preferences = yaml.load(file, Loader=yaml.FullLoader)

        self.preferences = preferences

    def load_data():
        """Load transaction data as DataFrame"""
        data = pd.read_csv("../intermediate/test.csv")
        df = pd.DataFrame(data)
        self.df = df

    def calculate_fee():
        """Calculate (sum and mean) fees, globally and per platform"""

        # Global fees (all platforms)
        feeSum = df['Fee amount EUR'].sum()
        feeMean = df['Fee amount EUR'].mean()

        # Total fees per platform
        feeSumPlatform = {}
        for platform in platforms:
            feeSumPlatform[platform] = df.loc[df['Destination platform'] == platform, 'Fee amount EUR'].sum()

        # Mean fees per platform
        feeMeanPlatform = {}
        for platform in platforms:
            feeMeanPlatform[platform] = df.loc[df['Destination platform'] == platform, 'Fee amount EUR'].mean()

        self.fee = {
            'sum': feeSum,
            'mean': feeMean,
            'feeSumPlatform': feeSumPlatform,
            'meanPlatform': feeMeanPlatform,
        }


    def originalValueCalc(df, platform=None, crypto=None):
        """Compute original investment"""

        selectionDeposit = df['Origin currency'] == 'EUR'
        selectionWithdrawal = df['Destination currency'] == 'EUR'

        if platform:
            selectionDeposit = selectionDeposit & (df['Destination platform'] == platform)
            selectionWithdrawal = selectionWithdrawal & (df['Origin platform'] == platform)

        if crypto:
            selectionDeposit = selectionDeposit & (df['Destination currency'] == crypto)
            selectionWithdrawal = selectionWithdrawal & (df['Origin currency'] == crypto)

        valueDeposit = df.loc[selectionDeposit, 'Origin amount'].sum()
        valueWithdrawal = df.loc[selectionWithdrawal, 'Destination amount'].sum()
        originalValue = valueDeposit - valueWithdrawal

        return originalValue


    def calculate_gain():
        """Calculate gain"""

        self.gain = {1}


    def calculate_equivalents():
        equivalents = self.preferences['equivalents']
        for equivalent in equivalents:
            for eq_name, eq_value in equivalent.items():
                self.eq_perf[eq_name]['total'] = self.gain{'total'} / eq_value
                for platform in platforms:
                    self.eq_perf[eq_name][platform] = self.gain[platform] / eq_value


    def perf_by_topic():
        for topic, cryptos in topics.items():
            kpiTopic[topic] = originalValueCalc(df, cryptos=cryptos)


    def processor(df):
        """Compute indicators from raw data"""

        load_preferences()

        load_data()

        self.platforms = pd.unique(df['Destination platform'])

        calculate_fee()
        calculate_gain()
        calculate_equivalents()
        perf_by_topic()

        renderData = {
            'data': data,
            'fee': fee,
            'gain': gain,
            'kpi': indicator,
            'kpiTopic': kpiTopic,
        }

        return {
            self.fee,
            self.gain,
        }






# Lecture
#--------

data = pd.read_csv("../intermediate/test.csv")
df = pd.DataFrame(data)


# Trouver le nombre de plateformes
platforms = pd.unique(df['Destination platform'])
nPlatforms = len(platforms)

# Choisir ligne tq origine des fonds == Binance
df[df['Origin platform'] == platforms[0]]


# Frais
#------

# Frais totaux
feeSum = df['Fee amount EUR'].sum()

# Frais totaux par plateforme
feeSumCoinbase = df.loc[df['Destination platform'] == 'CoinBase', 'Fee amount EUR'].sum()
for platform in platforms:
    feeSumPlatform[platform] = df.loc[df['Destination platform'] == platform, 'Fee amount EUR'].sum()

# Frais moyens par plateforme
feeMean = df['Fee amount EUR'].mean()
feeMeanPlatform = {}
for platform in platforms:
    feeMeanPlatform[platform] = df.loc[df['Destination platform'] == platform, 'Fee amount EUR'].mean()

feeMeanCoinbase = df.loc[df['Destination platform'] == 'CoinBase', 'Fee amount EUR'].mean()
feeMeanUphold = df.loc[df['Destination platform'] == 'UpHold', 'Fee amount EUR'].mean()

fee = {
    'sum': feeSum,
    'mean': feeMean,
    'meanPlatform': feeMeanPlatform,
}

# Gains
#------

def originalValueCalc(df, platform=None, crypto=None):
    """Compute origial investment"""
    # Todo : Add case where currency is not EUR and platform origin is not a single bank

    selectionDeposit = df['Origin currency'] == 'EUR'
    selectionWithdrawal = df['Destination currency'] == 'EUR'

    if platform:
        selectionDeposit = selectionDeposit & (df['Destination platform'] == platform)
        selectionWithdrawal = selectionWithdrawal & (df['Origin platform'] == platform)

    if crypto:
        selectionDeposit = selectionDeposit & (df['Destination currency'] == crypto)
        selectionWithdrawal = selectionWithdrawal & (df['Origin currency'] == crypto)

    valueDeposit = df.loc[selectionDeposit, 'Origin amount'].sum()
    valueWithdrawal = df.loc[selectionWithdrawal, 'Destination amount'].sum()
    originalValue = valueDeposit - valueWithdrawal

    return originalValue







    if platform:
        selection = df['Destination platform'] == platform & df['Destination currency'] == cryptos
        valueDeposit = df.loc[selection, 'Destination amount'].sum()

        selection = df['Origin platform'] == platform & df['Origin currency'] == cryptos
        valueWithdrawal = df.loc[selection, 'Origin amount'].sum()

        originalValue = valueDeposit - valueWithdrawal

    else:
        originalValue = df.loc[df['Origin platform'] == 'Boursorama', 'Origin amount'].sum()
    return originalValue

def currentValueCalc(df, platform=None):
    """Compute investment current value"""
    currentValue = 2000
    return currentValue

currentValue = currentValueCalc(df)
originalValue = originalValueCalc(df)

originalValuePlatform = {}
for platform in platforms:
    originalValuePlatform[platform] = originalValueCalc(df, platform=platform)

for platform in platforms:
    currentValuePlatform[platform] = currentValueCalc(df, platform=platform)

gain = currentValue - originalValue
performance = (currentValue - originalValue) / originalValue * 100

for platform in platforms:
    gainPlatform = currentValuePlatform[platform] - originalValuePlatform[platform]

for platform in platforms:
    performancePlatform = (currentValuePlatform[platform] - originalValuePlatform[platform]) / originalValuePlatform[platform] * 100

gain = {
    'gain': gain,
    'performance': performance,
    'gainPlatform': gainPlatform,
    'performancePlatform': performancePlatform,
}


# Indicateurs
#------------

salaire = 2000
kpiSalary = gain / salaire
kpiSalaryPlatform = {}
for platform in platforms:
    kpiSalaryPlatform = gainPlatform[platform] / salaire

bonnat = 5
kpiBonnat = gain / bonnat
for platform in platforms:
    kpiSalaryPlatform = gainPlatform[platform] / salaire

kpiReward = df.loc[df['Destination platform'] == 'CoinBase', 'Fee amount EUR'].sum()

kpi = {
    'salary': {
        'total': kpiSalary,
        kpiSalaryPlatform,
    },
    'bonnat': kpiBonnat,
    'reward': kpiReward,
}


# KPI by topic
#-------------

topics = {
    'payment': ['BTC', 'ETH', 'XRP', 'BCH', 'LTC', 'LINK', 'EOS', 'XLM', 'ADA',
                'XMR', 'TRX', 'DASH', 'ETC', 'ZEC', 'BAT', 'ALGO', 'ICX', 'WAVES',
                'OMG', 'GNO', 'MLN', 'NANO', 'DOGE', 'USDT', 'DAI', 'SC', 'LSK',
                'XTZ', 'ATOM', 'REP'],
    'infrastructure': ['ETH', 'ETC', 'ICX', 'EOS', 'OMG', 'ADA', 'TRX', 'ALGO',
                       'WAVES', 'LSK', 'XTZ', 'ATOM'],
    'financial': ['GNO', 'MLN', 'REP', 'COMP', 'LEND'],
    'service': ['LINK', 'SC', 'STORJ'],
    'entertainment': ['BAT'],
}


for topic, cryptos in topics.items():
    kpiTopic[topic] = originalValueCalc(df, cryptos=cryptos)


# Data to be transmitted to output
#---------------------------------

renderData = {
    'data': data,
    'fee': fee,
    'gain': gain,
    'kpi': kpi,
    'kpiTopic': kpiTopic,
}
