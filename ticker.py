from doctest import DocFileSuite
from re import L
from urllib.parse import urljoin
from copy import deepcopy
import pandas as pd
from typing import Optional, Union, List, Dict, Callable
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
from ._abstract import AbstractAPI
from .utils.config import Config

DEFAULT_CONFIG = "./FinancialModelingPrep/.config/config.json"
QUARTER_END = {1: (3, 31), 
    2: (6, 30),
    3: (9, 30),
    4: (12, 31)}
TODAY = dt.datetime.today()
NOW = dt.datetime.now()
CUR_YEAR = TODAY.year
LAST_Q = (TODAY - dt.timedelta(days=90)).month // 3
class Ticker(AbstractAPI):

    def __init__(self, ticker: str, 
        config: Union[str, Config]=DEFAULT_CONFIG, 
        mode='statements',
        **kwargs):
        super(Ticker, self).__init__(config=config,
            **kwargs)
        self.available_tickers = self._get_available_tickers(mode=mode)
        if isinstance(ticker, str):
            if "," in ticker: 
                tickers = ticker.upper().split(",")
                tickers = [t.strip() for t in tickers]
                assert len(tickers) == sum([t in self.available_tickers 
                    for t in tickers]), \
                    f"All tickers must be available! These are not valid tickers: {' '.join([t for t in tickers if t not in self.available_tickers])}"
                self.tickers = tickers
            else:
                assert ticker.upper().strip() in [t.upper() for t in self.available_tickers], "Not a valid ticker!"
                self.tickers = [ticker.upper()]
        elif isinstance(ticker, list):
            assert len(tickers) == sum([str(t).upper().strip() in self.available_tickers 
                    for t in tickers]), \
                    f"All tickers must be available! These are not valid tickers: {' '.join([t for t in tickers if t not in self.available_tickers])}"
            self.tickers = [str(t).upper() for t in ticker]
        self.tickers_str = ",".join(self.tickers)

    def __get_statements(self, statement='income', 
        freq="A", 
        save_to_sql: bool=False, 
        limit: int=100) -> Optional[pd.DataFrame]:
        """interface for getting income/balance sheet/cash flow statements
        :param statement: takes 'income', 'balance_sheet', 'cashflow'
        :param freq: takes 'A' or 'Q'
        """
        params = deepcopy(self._default_params)
        params.update(dict(limit=limit))
        endpoints = dict(income='income-statement/', 
            balance_sheet='balance-sheet-statement/',
            cashflow='cash-flow-statement/')
        if freq == "A":
            res = self._get_data(url=urljoin(endpoints.get(statement),
                f"{','.join(self.tickers)}/"),
                limit=limit)
        elif freq == 'Q':
            res = self._get_data(url=urljoin(endpoints.get(statement),
                f"{','.join(self.tickers)}/"),
                period='quarter', limit=limit)
        else:
            raise NotImplementedError
        if isinstance(res, list):
            ls = []
            for entry in res:
                s = pd.Series(entry)
                ls.append(s.to_frame().T)
            df = pd.concat(ls).T
            df = df.T.set_index(['date', 'symbol', 'reportedCurrency', 'cik', 
                'fillingDate', 'acceptedDate', 'calendarYear', 'period']).T
            df = df.stack(['symbol', 'cik']).swaplevel(0, 2)
            if save_to_sql:
                assert self.sql_conn is not None, "sql_path must be specified if you want to use 'save_to_sql'"
                start = f"{df.columns.get_level_values('calendarYear')[0]}\
                    {df.columns.get_level_values('period')[0]}"
                end = f"{df.columns.get_level_values('calendarYear')[-1]}\
                    {df.columns.get_level_values('period')[-1]}"
                tablename = f"{'_'.join(self.ticker)}_{statement}_{freq}_{start}_{end}"
                df.to_sql(tablename, 
                    self._cur, if_exists="replace")
            return df
        else:
            raise TypeError("value returned from API is not a list")

    def get_income_statements(self, freq="A", save_to_sql: bool=False, 
        limit: int=100) -> Optional[pd.DataFrame]:
        """get income statement
        :param freq: takes 'A' or 'Q'
        """
        return self.__get_statements(statement='income', 
            freq=freq, save_to_sql=save_to_sql, limit=limit)

    def get_balance_sheet(self, freq="A", save_to_sql: bool=False, 
        limit: int=100) -> Optional[pd.DataFrame]:
        """get balance sheet statement
        :param freq: takes 'A' or 'Q'
        """
        return self.__get_statements(statement='balance_sheet', 
            freq=freq, save_to_sql=save_to_sql, limit=limit)

    def get_cashflow(self, freq="A", save_to_sql: bool=False,
        limit: int=100) -> Optional[pd.DataFrame]:
        """get cash flow statement
        :param freq: takes 'A' or 'Q'
        """
        return self.__get_statements(statement='cashflow', 
            freq=freq, save_to_sql=save_to_sql, limit=limit)
    
    def get_product_segments(self, freq='A'):
        """get the product segments for the ticker
        :param freq: takes 'A' or 'Q'

        Note: This temporarily resets the endpoint to v4 and sets it back to 
        v3 after the function call ends. This may cause issues when using other 
        functions with this concurrently.
        """
        # TODO - fix concurrency issue cause by the temporary endpoint reset
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        url = "revenue-product-segmentation/"
        if freq == 'A':
            d = self._get_data(url=url, ticker=",".join(self.tickers))
        elif freq == 'Q':
            d = self._get_data(url=url, ticker=",".join(self.tickers), 
                period='quarter')
        else:
            self.endpoint = endpoint
            raise NotImplementedError
        self.endpoint = endpoint # set endpoint back to v3
        return d

    def get_geo_segments(self, freq='A'):
        """get the geographical segments for the ticker
        :param freq: takes 'A' or 'Q'

        Note: This temporarily resets the endpoint to v4 and sets it back to 
        v3 after the function call ends. This may cause issues when using other 
        functions with this concurrently.
        """
        # TODO - fix concurrency issue cause by the temporary endpoint reset
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        url = "revenue-geographic-segmentation/"
        if freq == 'A':
            d = self._get_data(url=url, ticker=",".join(self.tickers))
        elif freq == 'Q':
            d = self._get_data(url=url, ticker=",".join(self.tickers), 
                period='quarter')
        else:
            self.endpoint = endpoint
            raise NotImplementedError
        self.endpoint = endpoint # set endpoint back to v3
        return d

    def get_transcripts(self, year: int, quarter: Optional[int]=None):
        """get earnings call transcript
        :param year: year of the earnings call
        :param quarter: takes 1, 2, 3, 4
        """
        if quarter:
            url = urljoin("earning_call_transcript/", ",".join(self.tickers))
            assert quarter in range(1, 5), "quarter must be between 1 and 4"
            return self._get_data(url=url, year=year, quarter=quarter)
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        url = urljoin("batch_earning_call_transcript/", ",".join(self.tickers))
        res = self._get_data(url=url, year=year)
        self.endpoint = endpoint
        return res

    @classmethod
    def download_transcripts(cls, ticker: str, 
        year: int, quarter: Optional[int]=None):
        """classmethod version of get_transcripts. Takes the same arguments
        To use this, you must make sure the apikey is saved through:
            ./FinancialModelingPrep/.config/config.json
        Otherwise the api will not return
        """
        return cls(ticker, DEFAULT_CONFIG).get_transcripts(year=year, quarter=quarter)

    def get_inst_ownership(self, incl_cur_q: bool=True, 
        save_to_sql: bool=False,):
        """get number of shares held by institutional shareholders disclosed 
        through 13F
        :param incl_cur_q: Include current Q or not
        """
        # TODO - solve concurrency issue caused by the temporary endpoint reset
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        url = "institutional-ownership/symbol-ownership"
        res = self._get_data(url=url, ticker=",".join(self.tickers),
            includeCurrentQuarter=incl_cur_q)
        if isinstance(res, list):
            ls = []
            for entry in res:
                s = pd.Series(entry)
                ls.append(s.to_frame().T)
            df = pd.concat(ls).T
            df = df.T.set_index(['date', 'symbol', 'cik',]).T
            df = df.stack(['symbol', 'cik']).swaplevel(0, 2)
            if save_to_sql:
                start = f"{df.columns.get_level_values('date')[0]}"
                end = f"{df.columns.get_level_values('date')[-1]}"
                tablename = f"{'_'.join(self.ticker)}_ownership_{start}_{end}"
                df.to_sql(tablename, 
                    self._cur, if_exists="replace")
            return df
        else:
            raise TypeError("value returned from API is not a list")

    @classmethod
    def list_inst_ownership(cls, ticker: str, incl_cur_q: bool=True):
        """classmethod version of get_ownership"""
        return cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_inst_ownership(incl_cur_q=incl_cur_q)

    def get_inst_owners(self, year: int=CUR_YEAR,
        quarter: int=LAST_Q,
        save_to_sql: bool=False,
        max_workers: int=8):
        """get number of shares held by institutional shareholders disclosed 
        through 13F
        :param incl_cur_q: Include current Q or not
        """
        # TODO - solve concurrency issue caused by the temporary endpoint reset
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        url = "institutional-ownership/institutional-holders/symbol-ownership-percent"
        def get_page(page: int=0):
            month, day = QUARTER_END.get(quarter)
            date = dt.date(year, month, day).strftime("%Y-%m-%d")
            res = self._get_data(url=url, ticker=",".join(self.tickers),
                page=page,
                date=date)
            if res: return res
        page, i = 1, 0
        res = []
        
        if max_workers > 1:
            with ThreadPoolExecutor() as executor:
                while page:
                    futures = [executor.submit(get_page, p) 
                        for p in range(i, i + max_workers)]
                    for future in as_completed(futures):
                        page = future.result()
                        if isinstance(page, list):
                            res += page
                    i += max_workers
        else:
            while page:
                page = get_page(i)
                if isinstance(page, list): res += page
                i += 1

        if isinstance(res, list):
            ls = []
            for entry in res:
                s = pd.Series(entry)
                ls.append(s.to_frame().T)
            df = pd.concat(ls).T
            df = df.T.set_index(['date', 'symbol', 'cik',]).T
            # df = df.stack(['symbol', 'cik']).swaplevel(0, 2)
            return df

        if save_to_sql:
            start = f"{df.columns.get_level_values('date')[0]}"
            end = f"{df.columns.get_level_values('date')[-1]}"
            tablename = f"{'_'.join(self.ticker)}_ownership_{start}_{end}"
            df.to_sql(tablename, 
                self._cur, if_exists="replace")
            return df
        else:
            raise TypeError("value returned from API is not a list")

    @classmethod
    def list_inst_owners(cls, ticker: str, year: int=CUR_YEAR, 
        quarter: int=LAST_Q, max_workers: int=8):
        """classmethod version of get_ownership"""
        return cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_inst_owners(year=year, 
                quarter=quarter, max_workers=max_workers)

    def __get_v4_info(self, url: str):
        """template function for getting v4 info"""
        endpoint = self.endpoint
        self.endpoint = endpoint.replace("v3", "v4")
        res = self._get_data(url=url, ticker=",".join(self.tickers))
        self.endpoint = endpoint
        return res

    def get_peers(self):
        """get the stock's peers"""
        url = "stock_peers"
        res = self.__get_v4_info(url=url)
        return res

    @classmethod
    def list_peers(cls, ticker: Union[str, List[str]]):
        """classmethod version of get_peers"""
        res = cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_peers()
        return res

    def get_core_info(self):
        """get the stock's core information such as cik, exchange, industry"""
        url = "company-core-information"
        res = self.__get_v4_info(url=url)
        return res

    def get_profile(self):
        """get company's profile information"""
        url = urljoin("profile/", ",".join(self.tickers))
        res = self._get_data(url=url)
        if isinstance(res, list):
            df = pd.concat([pd.Series(s).to_frame().T for s in res])
            df = df.set_index(["symbol"])
            return df
        else: return res

    @classmethod
    def company_profile(cls, ticker: Union[str, List[str]]):
        """classmethod version of get_profile"""
        res = cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_profile()
        return res

    def get_execs(self):
        """get list of key executives, their positions and bios"""
        url = urljoin("key-executives/", ",".join(self.tickers))
        res = self._get_data(url=url)
        if isinstance(res, list):
            df = pd.concat(pd.Series(d).to_frame().T for d in res)
            df.index = range(df.shape[0])
            return df
        else: return res
    
    @classmethod
    def list_execs(cls, ticker: Union[str, List[str]]):
        """classmethod version of get_execs"""
        return cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_execs()

    def get_financial_ratios(self, limit: int=10, 
        freq: str="A") -> Union[pd.DataFrame, list]:
        """get financial ratios in the statements
        :param limit: number of period going back
        :param freq: takes 'A' or 'Q'
        """
        url = urljoin("ratios/", self.tickers_str)
        if freq == 'A':
            res = self._get_data(url=url)
        elif freq == 'Q':
            res = self._get_data(url=url, period='quarter')
        else:
            raise NotImplementedError
        if isinstance(res, list):
            df = pd.concat([pd.Series(d).to_frame().T for d in res])
            return df
        else:
            return res

    @classmethod
    def download_financial_ratios(cls, ticker: str, 
        limit: int=10, freq: str='A') -> Union[pd.DataFrame, list]:
        """classmethod version of get_financial_ratios"""
        return cls(ticker=ticker, 
            config=DEFAULT_CONFIG).get_financial_ratios(limit=limit, freq=freq)
