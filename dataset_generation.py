"""generate data for downstream pipelines such as deep macro and NLP"""
from __future__ import annotations
import numpy as np
from typing import (Union, Sequence, List, Iterator, Dict, Callable)
import pandas as pd
from abc import abstractclassmethod, abstractmethod, ABC
from .tickers import Ticker
from .forex import ForEx

class abstract(ABC):
    def __init__(self,
        ):
        super(abstract, self).__init__()
    
    @abstractclassmethod
    def download_data(self):
        raise NotImplementedError

    


class CompanyProfiles(abstract):
    def __init__(self):
        super(CompanyProfiles, self).__init__()
        
    @classmethod
    def download_data(cls):
        profiles = Ticker.get_all_company_profiles()
        fx = ForEx.get_live_fx().T.price.to_dict()
        profiles.loc[:, "currency_code"] = profiles.currency.apply(lambda x: f"{x}USD" if x not in ["2.4", np.nan] else np.nan)
        profiles.loc[:, "fx"] = profiles.currency_code.map(fx)
        return profiles
        # profiles.loc[:, "market_cap_usd"]


if __name__ == "__main__":
    CompanyProfiles.download_data()

