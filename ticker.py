from urllib.parse import urljoin
from copy import deepcopy
import pandas as pd
from typing import Optional, Union, List, Dict, Callable
from ._abstract import AbstractAPI
from .utils.config import Config


class Ticker(AbstractAPI):

    def __init__(self, ticker: str, 
        config: Union[str, Config]="./FinancialModelingPrep/.config/config.json", 
        mode='statements',
        **kwargs):
        super(Ticker, self).__init__(config=config,
            **kwargs)
        self.available_tickers = self._get_available_tickers(mode=mode)
        if isinstance(ticker, str):
            if "," in ticker: 
                tickers = ticker.upper().split(",")
                assert len(tickers) == sum([t in self.available_tickers 
                    for t in tickers]), \
                    f"All tickers must be available! These are not valid tickers: {' '.join([t for t in tickers if t not in self.available_tickers])}"
                self.tickers = tickers.upper()
            else:
                assert ticker.upper().strip() in [t.upper() for t in self.available_tickers], "Not a valid ticker!"
                self.tickers = [ticker.upper()]
        elif isinstance(ticker, list):
            assert len(tickers) == sum([str(t).upper() in self.available_tickers 
                    for t in tickers]), \
                    f"All tickers must be available! These are not valid tickers: {' '.join([t for t in tickers if t not in self.available_tickers])}"
            self.tickers = [str(t).upper for t in ticker]

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

    def get_ownership(self, incl_cur_q: bool=True, save_to_sql: bool=False,):
        """interface for getting income/balance sheet/cash flow statements
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