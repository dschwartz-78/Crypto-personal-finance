import pandas as pd
import yaml
import numpy
# for debugging
from pprint import pprint


class Processor:
    """Class to process the history DataFrame"""

    # Create attributes (to be filled during different computation steps)
    platforms = numpy.ndarray
    cryptos = numpy.ndarray
    preferences = {}
    topics = {}
    df = pd.DataFrame()
    fee = {}
    investment = {}
    gain = {}
    eq_perf = {}
    perf_topic = {}


    def load_preferences(self):
        """Load values from preferences file input/preferences.yaml"""

        # Load default values
        preferences = {
            'indicators': [
                {'salary': 2000},
                {'kebab': 5}
            ],
            'output': [
                {'skin': 'skin-2020'}
            ],
        }

        # Read YAML preferences file
        pref_file = 'input/preferences.yaml'
        with open(pref_file) as file:
            preferences = yaml.load(file, Loader=yaml.FullLoader)

        self.preferences = preferences

    def load_data(self):
        """Load transaction data as DataFrame"""
        # todo : remove function as df will be given as parameter from reader.py
        data = pd.read_csv("intermediate/test.csv")
        df = pd.DataFrame(data)
        self.df = df

    def load_topics(self):
        """Load topics list file source/topics.yaml"""

        # Load default values
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

        # Read YAML topics file
        topics_file = 'source/topics.yaml'
        with open(topics_file) as file:
            topics = yaml.load(file, Loader=yaml.FullLoader)

        return topics

    def calculate_fee(self):
        """Calculate (sum and mean) fees, globally and per platform"""

        # Global fees (all platforms)
        feeSum = self.df['Fee amount EUR'].sum()
        feeMean = self.df['Fee amount EUR'].mean()

        # Total fees per platform
        feeSumPlatform = {}
        for platform in self.platforms:
            feeSumPlatform[platform] = self.df.loc[self.df['Destination platform'] == platform, 'Fee amount EUR'].sum()

        # Mean fees per platform
        feeMeanPlatform = {}
        for platform in self.platforms:
            feeMeanPlatform[platform] = self.df.loc[self.df['Destination platform'] == platform, 'Fee amount EUR'].mean()

        self.fee = {
            'sum': feeSum,
            'mean': feeMean,
            'feeSumPlatform': feeSumPlatform,
            'meanPlatform': feeMeanPlatform,
        }

    def calculate_investment(self, df, platform=None, crypto=None):
        """Compute original investment over all platforms, by platform and by crypto"""
        # Combien d'argent j'ai mis : en tout, par plateforme, par crypto

        selectionDeposit = pd.Series([True] * len(self.df))
        selectionWithdrawal = pd.Series([True] * len(self.df))
        if not platform and not crypto:
            selectionDeposit = df['Origin currency'] == 'EUR'
            selectionWithdrawal = df['Destination currency'] == 'EUR'

        if platform:
            selectionDeposit = selectionDeposit & (df['Destination platform'] == platform)
            selectionWithdrawal = selectionWithdrawal & (df['Origin platform'] == platform)

        if crypto:
            selectionDeposit = selectionDeposit & (df['Destination currency'] == crypto)
            selectionWithdrawal = selectionWithdrawal & (df['Origin currency'] == crypto)

        valueDeposit = df.loc[selectionDeposit, 'Origin amount EUR'].sum()
        valueWithdrawal = df.loc[selectionWithdrawal, 'Origin amount EUR'].sum()
        investment = valueDeposit - valueWithdrawal

        return investment

    def calculate_balance(self, df, platform=None, crypto=None):
        """Compute balance (asset current value)"""
        # Combien d'argent ça vaut actuellement : en tout, par plateforme, par crypto
        # Pour cela il faut se connecter en ligne via une API.

        # todo
        balance = 1000
        if not platform and not crypto:
            balance = 2000

        if platform == 'CoinBase':
            balance = 1000
        if platform == 'UpHold':
            balance = 1000

        return balance

    def calculate_gain(self):
        """Calculate gain for all, by platform and by crypto"""

        # Overall gain
        investment = self.calculate_investment(self.df)
        balance = self.calculate_balance(self.df)

        gain = balance - investment
        performance = (balance - investment) / investment * 100
        performance = round(performance, 2)     # round 2 digits after comma

        # Gain by platform
        # todo : traiter le cas où la somme d'un investissement est nulle (par ex. quitté
        # une plateforme ou un position) car implique une division par 0 lors du calcul
        # de la performance.
        investmentPlatform = {}
        for platform in self.platforms:
            investmentPlatform[platform] = self.calculate_investment(self.df, platform=platform)

        balancePlatform = {}
        for platform in self.platforms:
            balancePlatform[platform] = self.calculate_balance(self.df, platform=platform)

        gainPlatform = {}
        for platform in self.platforms:
            gainPlatform[platform] = balancePlatform[platform] - investmentPlatform[platform]

        performancePlatform = {}
        for platform in self.platforms:
            performancePlatform[platform] = (balancePlatform[platform] - investmentPlatform[platform]) / investmentPlatform[platform] * 100
            performancePlatform[platform] = round(performancePlatform[platform], 2)

        # Gain by crypto
        investmentCrypto = {}
        for crypto in self.cryptos:
            investmentCrypto[crypto] = self.calculate_investment(self.df, crypto=crypto)

        balanceCrypto = {}
        for crypto in self.cryptos:
            balanceCrypto[crypto] = self.calculate_balance(self.df, crypto=crypto)

        gainCrypto = {}
        for crypto in self.cryptos:
            gainCrypto[crypto] = balanceCrypto[crypto] - investmentCrypto[crypto]

        performanceCrypto = {}
        for crypto in self.cryptos:
            performanceCrypto[crypto] = (balanceCrypto[crypto] - investmentCrypto[crypto]) / investmentCrypto[crypto] * 100
            performanceCrypto[crypto] = round(performanceCrypto[crypto], 2)

        investment = {
            'total': investment,
            'investmentPlatform': investmentPlatform,
            'investmentCrypto': investmentCrypto,
        }
        self.investment = investment

        gain = {
            'total': gain,
            'performance': performance,
            'gainPlatform': gainPlatform,
            'performancePlatform': performancePlatform,
            'gainCrypto': gainCrypto,
            'performanceCrypto': performanceCrypto,
        }
        self.gain = gain

    def calculate_equivalents(self):
        equivalents = self.preferences['equivalents']
        for equivalent in equivalents:
            for eq_name, eq_value in equivalent.items():
                self.eq_perf[eq_name] = {}
                self.eq_perf[eq_name]['total'] = self.gain['total'] / eq_value
                for platform in self.platforms:
                    self.eq_perf[eq_name][platform] = self.gain['gainPlatform'][platform] / eq_value

    def perf_by_topic(self):
        """Calculate performaces by topic"""

        topics = self.load_topics()

        # pprint(self.gain)
        perf_topic = {}
        for topic, cryptos in topics.items():
            print(topic)
            perf_topic_tmp = numpy.ndarray([])
            weight = numpy.ndarray([])
            print(str(weight))
            for crypto in cryptos:
                print('  ' + crypto)
                print('    ' + str(weight))
                if crypto in self.cryptos:
                    weight = numpy.append(weight, self.investment['investmentCrypto'][crypto])
                    perf_topic_tmp = numpy.append(perf_topic_tmp, self.gain['performanceCrypto'][crypto])
                    print('    ' + str(weight))
            print('  topic weight : ' + str(weight))
            print('  topic perf_topic_tmp : ' + str(perf_topic_tmp))
            perf_topic[topic] = numpy.sum(perf_topic_tmp * weight / weight.sum())
            pprint(perf_topic[topic])

        self.perf_topic = perf_topic

    def processor(self):
        """Compute indicators from raw data"""

        self.load_preferences()

        self.load_data()

        self.platforms = pd.unique(self.df['Destination platform'])
        self.cryptos = pd.unique(self.df['Destination currency'])

        # Keep the execution order
        self.calculate_fee()
        self.calculate_gain()
        self.calculate_equivalents()
        self.perf_by_topic()
        print('##############')
        pprint(self.perf_topic)
        print('##############')
        quit()

        renderData = {
            'data': self.df,
            'fee': self.fee,
            'gain': self.gain,
            'eq_perf': self.eq_perf,
            'perf_topic': self.perf_topic,
        }

        pprint.pprint(renderData)

        return renderData


p = Processor()
renderData = p.processor()
