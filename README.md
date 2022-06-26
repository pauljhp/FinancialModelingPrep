# Wrapper for the Financial Modeling Prep API
version 0.0.1

## Structure
- 3 main classes are implemented:
    - ticker.Ticker for getting stock level information;
    - funds.Funds for getting information about funds;
    - indices.Index for getting information about indices. 
- All classes inherits from the base class _abstract.AbstractAPI

## Style guide
- Use type hints for type checks and readability
- CamelCase classes, all file and method names should be lower cases, connected by \_;
- Global variables shoudl be in UPPER CASE;
- Seperate classes with 2 empty lines; seperate method with 1 empty line;