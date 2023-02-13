from __future__ import annotations
from urllib.parse import urljoin
from copy import deepcopy
import re
import pandas as pd
from typing import (Optional, Union, List, Dict, Callable, Sequence)
from collections import deque
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
from argparse import ArgumentParser
from pathlib import Path
import math
from ._abstract import AbstractAPI
from .utils.config import Config
from .utils.utils import pandas_strptime, iter_by_chunk


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

config_p = Path(DEFAULT_CONFIG)
if not config_p.exists():
    config_p.parent.mkdir(parents=True, exist_ok=True)
    config_p.write_text(r"""{{"apikey": "{a}"}}""".format(
    a=input("the config file wasn't found - enter your apikey: "))
    )


class ForEx(AbstractAPI):

    def __get_available_pairs(self):
        res = self._get_data(url="symbol/available-forex-pairs")
        if isinstance(res, List):
            print(res) # FIXME
            df = pd.concat([pd.Series(d).to_frame().T for d in res]).T
            return df
        else: return res

    def __init__(self, 
        config: Union[str, Callable, Config]=DEFAULT_CONFIG,
        **kwargs):
        super(ForEx, self).__init__(config=config)

    @property
    def available_tickers_(self):
        """returns available tickers"""
        self._available_tickers_ = self.__get_available_pairs()
        return self._available_tickers_

    def _get_live_fx(self) -> Union[Dict, pd.DataFrame]:
        res = self._get_data(url="quotes/forex")
        if isinstance(res, List):
            df = pd.concat([pd.Series(d).to_frame().T for d in res]).T
            return df
        else: return res

    @classmethod
    def get_live_fx(cls, 
        config: Union[str, Callable, Config]=DEFAULT_CONFIG) -> Union[Dict, pd.DataFrame]:
        return cls(config=config)._get_live_fx()

    def _get_historical_fx(self,
        ticker: Union[str, Sequence[str]],
        freq: str="d") -> pd.DataFrame:
        """get the historical fx rate
        :param ticker: takes list or str. must be in the `self.available_tickers_ ` list
        :param freq: takes the following arguments:
            - "d", "daily", "day" - daily frequency
            - "Xmin" for X in [1, 5, 15, 30]
            - "Xhour" for X in [1, 4]
        """
        chart_freqs = [f"{i}min" for i in (1, 5, 15, 30)] + \
            [f"{i}hour" for i in (1, 4)]
        if isinstance(ticker, str):
            ticker_str = ticker
        elif isinstance(ticker, Sequence):
            ticker_str = ",".join(ticker)
        else:
            raise TypeError("`ticker` must be either string or sequence of string!")

        if freq in ["d", "daily", "day"]:
            res = self._get_data(url=f"historical-price-full/{ticker_str}")
            if isinstance(res, Dict):
                if 'historicalStockList' in res.keys():
                    ls = []
                    for data in res.get("historicalStockList"):
                        symbol = data.get("symbol")
                        val = pd.concat((pd.Series(d).to_frame().T for d in data.get("historical")))
                        val.index = pd.MultiIndex.from_tuples([(symbol, date) for date in val.date])
                        ls.append(val)
                    df = pd.concat(ls)
                    return df
                else:
                    symbol = res.get("symbol")
                    val = pd.concat((pd.Series(d).to_frame().T for d in res.get("historical")))
                    val.index = pd.MultiIndex.from_tuples([(symbol, date) for date in val.date])
                    df = val
                    return df
            else:
                return res
        elif freq in chart_freqs:
            res = self._get_data(url=f"historical-chart/{freq}/{ticker_str}/")
            if isinstance(res, Dict):
                if 'historicalStockList' in res.keys():
                    ls = []
                    for data in res.get("historicalStockList"):
                        symbol = data.get("symbol")
                        val = pd.concat((pd.Series(d).to_frame().T for d in res.get("historical")))
                        val.index = pd.MultiIndex.from_tuples([(symbol, date) for date in val.date])
                        ls.append(val)
                    df = pd.concat(ls)
                    return df
                else:
                    symbol = res.get("symbol")
                    val = pd.concat((pd.Series(d).to_frame().T for d in data.get("historical")))
                    val.index = pd.MultiIndex.from_tuples([(symbol, date) for date in val.date])
                    df = val
                    return df
            else:
                return res
        else:
            raise ValueError("frequency specified not supported!")

    @classmethod
    def get_historical_fx(cls, 
        ticker: Union[str, Sequence[str]],
        config: Union[str, Callable, Config]=DEFAULT_CONFIG,
        freq: str="d"):
        return cls(config=config)._get_historical_fx(ticker, freq)
    
