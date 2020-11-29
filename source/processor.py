import pandas as pd


def processor(df):
    """Compute indicators from raw data"""


    return renderData






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
for platform in platforms:
    feeMeanPlatform[platform] = df.loc[df['Destination platform'] == platform, 'Fee amount EUR'].mean()

feeMeanCoinbase = df.loc[df['Destination platform'] == 'CoinBase', 'Fee amount EUR'].mean()
feeMeanUphold = df.loc[df['Destination platform'] == 'UpHold', 'Fee amount EUR'].mean()

fee = {
    'sum': feeSum,
    'mean': feeMean,
    'meanPlatform': feeMeanPlatform,
    # 'meanPlatform': {
    #     'Coinbase': feeMeanCoinbase,
    #     'Uphold': feeMeanUphold,
    # }
}

# Gains
#------

def originalValueCalc(df, platform=None, cryptos=None):
    """Compute origial investment"""
    # Todo : Add case where currency is not EUR and platform origin is not a single bank
    if platform:
        # toto : correct syntax
        originalValue = df.loc[df['Origin platform'] == platform & df['Origin currency'] == cryptos, 'Origin amount'].sum()
    else:
        originalValue = df.loc[df['Origin platform'] == 'Boursorama', 'Origin amount'].sum()
    return originalValue

def currentValueCalc(df, platform=None):
    """Compute investment current value"""
    currentValue = 2000
    return currentValue

currentValue = currentValueCalc(df)
originalValue = originalValueCalc(df)

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
