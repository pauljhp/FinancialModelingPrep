from __future__ import annotations
from urllib.parse import urljoin
from copy import deepcopy
import pandas as pd
from typing import Optional, Union, List, Dict, Callable
from collections import deque
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
from argparse import ArgumentParser
from pathlib import Path
from ._abstract import AbstractAPI
from .utils.config import Config
from .utils.utils import pandas_strptime, iter_by_chunk


LOGPATH = './FinancialModelingPrep/.log/'
LOGFILE = os.path.join(LOGPATH, 'log.log')

if not os.path.exists(LOGPATH):
    os.makedirs(LOGPATH)

logging.basicConfig(filename=LOGFILE, 
    # encoding='utf-8', 
    level=logging.DEBUG)

DEFAULT_CONFIG = "./FinancialModelingPrep/.config/config.json"
QUARTER_END = {
    1: (3, 31), 
    2: (6, 30),
    3: (9, 30),
    4: (12, 31)
    }
TODAY = dt.datetime.today()
NOW = dt.datetime.now()
CUR_YEAR = TODAY.year
LAST_Q = (TODAY - dt.timedelta(days=90)).month // 3
DEFAULT_START_DATE = dt.date(2020, 1, 1)


class Economics(AbstractAPI):

    def __init__(self, config: Union[str, Config]=DEFAULT_CONFIG):
        super(Economics, self).__init__(
            config=config,
            version='v4')
        self.all_fields = [
            'GDP', 
            'realGDP', 
            'nominalPotentialGDP',
            'realGDPPerCapita',
            'federalFunds',
            'CPI',
            'inflationRate', 
            'inflation',
            'retailSales',
            'consumerSentiment', 
            'durableGoods', 
            'unemploymentRate',
            'totalNonfarmPayroll', 
            'initialClaims',
            'industrialProductionTotalIndex',
            'newPrivatelyOwnedHousingUnitsStartedTotalUnits',
            'totalVehicleSales',
            'retailMoneyFunds', 'smoothedUSRecessionProbabilities', 
            '3MonthOr90DayRatesAndYieldsCertificatesOfDeposit', 
            'commercialBankInterestRateOnCreditCardPlansAllAccounts',
            '30YearFixedRateMortgageAverage',
            '15YearFixedRateMortgageAverage'
        ]

    def __get_data(self, field: str,
        start_date: Union[str, dt.date],
        end_date: Union[str, dt.date]):
        if isinstance(start_date, dt.date): start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, dt.date): end_date = end_date.strftime("%Y-%m-%d")
        return self._get_data(url="economic",
            additional_params={'from': start_date,
                'end': end_date},
            name=field)

    def get_data(self, field: str,
        start_date: Union[str, dt.date]=DEFAULT_START_DATE,
        end_date: Union[str, dt.date]=TODAY,
        freq: Optional[str]=None):
        data = self.__get_data(field, start_date, end_date)
        data = pd.DataFrame(data, columns=['date', 'value'])
        data = pandas_strptime(data, 
            index_name='date', axis=1)
        if freq:
            try:
                data.date = data.date.dt.to_period(freq)
            except Exception as e:
                logging.error(e)
        return data

    @classmethod
    def get_all_data(cls, 
        start_date: Union[str, dt.date]=DEFAULT_START_DATE, 
        end_date: Union[str, dt.date]=TODAY,
        max_workers: int=8,
        freq: str='M'):
        res = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(cls(DEFAULT_CONFIG).get_data, 
                    field, start_date, end_date, freq=freq)
                for field in cls(DEFAULT_CONFIG).all_fields]
        for future in as_completed(futures):
            data = future.result()
            res.append(data)
        
        return res
