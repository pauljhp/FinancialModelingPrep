# Wrapper for the Financial Modeling Prep API

version 0.0.2

## Change log

- Added multithreaded pagination for downloading shareholder lists

## Usage

- All sub modules are designed to work both as modules and as scripts

### 1. Ticker Class

- You can instantiate a Ticker class:

  > from FinancialModelingPrep.tickers import Ticker
  >

  > t = Ticker(`<YOUR TICKER>`) # you either input a single ticker (case insentitive), wrap multiple tickers seperated by ",", or wrap multiple tickers in a List[str]

- You can also use the class methods without instantiating:
  > Ticker.get_stock_news(<`TICKER`>, <`start_date`>)

- If you have specified a sqlite path, when setting save_to_sql=True, apart from returning a pd.DataFrame, the dataframe will also be written into the sql database
- You can also call classmethod Ticker().get_income_statements(ticker=`<YOUR TICKER>`) with our instantiating the class

## Structure

- 3 main classes are implemented:
  - ticker.Ticker for getting stock level information;
  - funds.Funds for getting information about funds;
  - indices.Index for getting information about indices.
- All classes inherit from the base class _abstract.AbstractAPI

## Style guide

- Use type hints for type checks and readability
- CamelCase classes, all file and method names should be lower cases, connected by \_;
- Global variables shoudl be in UPPER CASE;
- Seperate classes with 2 empty lines; seperate method with 1 empty line;
