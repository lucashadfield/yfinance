import sqlite3
from contextlib import closing
from pathlib import Path
import pandas as pd

PATH = Path('~/.config/yfinance/yfinance.db').expanduser()


def create_table(con):
    con.execute('''
        CREATE TABLE IF NOT EXISTS ticker_history (
            Date TIMESTAMP,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            `Adj Close` REAL,
            Volume INTEGER,
            Dividends INTEGER,
            `Stock Splits` INTEGER,
            Ticker TEXT,
            Updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (Date, Ticker)
        );
    ''')


def upsert(con):
    with con:
        create_table(con)
        con.execute('''
            INSERT OR REPLACE INTO ticker_history (
                Date,
                Open,
                High,
                Low,
                Close,
                `Adj Close`,
                Volume,
                Dividends,
                `Stock Splits`,
                Ticker
            )
            SELECT * 
            FROM ticker_history_staging;
        ''')


def cache_history(df, ticker):
    # create path if need be
    if not PATH.parent.exists():
        PATH.parent.mkdir(parents=True, exist_ok=True)

    # write to staging
    write_df = df.copy()
    write_df['Ticker'] = ticker
    with closing(sqlite3.connect(PATH)) as con:
        write_df[write_df['Close'].notnull()].reindex(columns=[
            'Open',
            'High',
            'Low',
            'Close',
            'Adj Close',
            'Volume',
            'Dividends',
            'Stock Splits',
            'Ticker'
        ]).to_sql('ticker_history_staging', con=con, if_exists='replace')

        # upsert to main
        upsert(con)

        return pd.read_sql('''
            SELECT *
            FROM ticker_history
            WHERE Date BETWEEN ? and ?
        ''', con=con, index_col='Date', params=[df.index.min().to_pydatetime(), df.index.max().to_pydatetime()], parse_dates='Date')[df.columns]

