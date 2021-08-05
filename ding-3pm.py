''' 
    > download groupedDaily bars from polygon
    > download dailyReference table from polygon
    > filter common stock from grouped daily, append to a historical table
    > pull historical table, applyT, send back to postgres
    > check filter criteria in the daily table, compose report of set ups

    > polygotter
    |   > _strfdateArray()
    |   > groupedDaily()
    |   > refTickerType()
    |   > gdRawCS_toSQL()
    |   > afternoons()
    > afternoonT
    |   > steep()
    > pandascan
    |   > pct48_KsubD_bbPw_RSI14pct24()
'''

import time
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


class polygotter : 


    def __init__(self) -> None:
        pass


    _pwd = 'finPal-app'
    today = date.today()
    strf_today = datetime.strftime(today, '%Y-%m-%d')
    url_base = 'https://api.polygon.io'
    apiKey = 'QLkG8Ff7Kupvv_M2h1RZxfrXzpMpoPdx'
    dbEngine = 'postgresql+psycopg2://postgres:s@unAsh*w3r@localhost/market'


    @classmethod
    def _strfdateArray(cls, stISOf, endISOf=strf_today):
        ''' stISOf and endISOf take "YYYY-MM-DD" as strings. '''
        fromDate = date.fromisoformat(stISOf)
        toDate = date.fromisoformat(endISOf)
        aDay = timedelta(days=1)
        prevDate = toDate - aDay
        date_array = [ datetime.strftime(prevDate, '%Y-%m-%d') ]
        while prevDate > fromDate:
            prevDate = prevDate - aDay
            date_array.append(datetime.strftime(prevDate, '%Y-%m-%d'))
        return date_array


    @classmethod
    def groupedDaily(cls, gdDate=strf_today, outDir=_pwd):
        ''' requestDate format is YYYY-MM-DD '''
        url_endpoint = 'v2/aggs/grouped/locale/us/market/stocks'
        url_adj = 'adjusted=false'
        url = f'{cls.url_base}/{url_endpoint}/{gdDate}?{url_adj}&apiKey={cls.apiKey}'
        print(f"zZzZz 15 seconds... @ {time.strftime('%H:%M:%S', time.localtime())}")
        time.sleep(15)
        response = requests.get(url).json()
        response_df = pd.json_normalize(response['results'])
        response_df.to_csv(f'{outDir}/polygon-gdBars/gd-{gdDate}.csv')
        print(f'response dataFrame >> {outDir}/polygon-gdBars/')
        return response_df


    @classmethod
    def refTickerType(cls, refTickerDate=strf_today, tickerType='CS', outDir=_pwd):
        ''' requestDate format is YYYY-MM-DD '''
        url_endpoint = 'v3/reference/tickers'
        url_format = ( 
            f'&market=stocks&date={refTickerDate}'
            f'&active=true&sort=ticker&order=desc&limit=1000'
        )
        url = f'{cls.url_base}/{url_endpoint}?type={tickerType}{url_format}&apiKey={cls.apiKey}'
        print(f"zZzZz 15 seconds... @ {time.strftime('%H:%M:%S', time.localtime())}")
        time.sleep(15)
        response = requests.get(url).json()
        response_df = pd.json_normalize(response['results'])
        responseFrames = []
        responseFrames.append(response_df)
        while 'next_url' in response:
            url_nextPage = response['next_url']
            url_nextRequest = f'{url_nextPage}&apiKey={cls.apiKey}'
            print( 
                f"zZzZz 15 seconds... @ "
                f"{time.strftime('%H:%M:%S', time.localtime())}" 
                )
            time.sleep(15)
            response = requests.get(url_nextRequest).json()
            response_df = pd.json_normalize(response['results'])
            responseFrames.append(response_df)
        out_df = pd.concat(responseFrames)
        out_df.to_csv(f'{outDir}/polygon-refTickerType/refCS-{refTickerDate}.csv')
        print(f'response dataFrame >> {outDir}/polygon-refTickerType/')
        return out_df


    @classmethod
    def gdRawCS_toSQL(cls, gdDir, refDir, to_dbTable, stISOf, endISOf=strf_today):
        date_array = cls._strfdateArray(stISOf=stISOf, endISOf=endISOf)
        outFrames = []
        for aDate in date_array:
            try:
                gd_df = pd.read_csv(f'{gdDir}/gd-{aDate}.csv', index_col=0)
                ref_df = pd.read_csv(f'{refDir}/refCS-{aDate}.csv', index_col=0)
                cs_list = ref_df['ticker'].tolist()
                gd_df = gd_df.loc[gd_df['T'].isin(cs_list)]
                gd_df.rename( 
                    columns={ 
                        'T': 'ticker',
                        'v': 'volume',
                        'vw': 'vwap',
                        'o': 'open',
                        'c': 'close',
                        'h': 'high',
                        'l': 'low',
                        't': 'timestamp',
                        'n': 'transactions'
                    },
                    inplace=True
                )
                gd_df['date'] = aDate
                outFrames.append(gd_df)
                print(f'{aDate}...')
            except:
                print(f'skipping {aDate}')
                continue
        out_df = pd.concat(outFrames, axis=0)
        engine = (create_engine(cls.dbEngine))
        out_df.to_sql(
            name=to_dbTable,
            con=engine,
            index=False,
            if_exists='append'
        )
        print(f'gd CS >> {to_dbTable}')


    @classmethod
    def afternoons(cls, to_dbTable):
        ''' this is for common stock. works best after 4:00pm EST. '''
        gd_df = cls.groupedDaily()
        ref_df = cls.refTickerType()
        cs_list = ref_df['ticker'].tolist()
        gd_df = gd_df.loc[gd_df['T'].isin(cs_list)]
        gd_df.rename( 
            columns={ 
                'T': 'ticker',
                'v': 'volume',
                'vw': 'vwap',
                'o': 'open',
                'c': 'close',
                'h': 'high',
                'l': 'low',
                't': 'timestamp',
                'n': 'transactions'
            },
            inplace=True
        )
        gd_df['date'] = cls.strf_today
        engine = (create_engine(cls.dbEngine))
        gd_df.to_sql( 
            name=to_dbTable,
            con=engine,
            index=False,
            if_exists='append'
        )
        print(f'gdCS >> {to_dbTable}')
        return gd_df


class afternoonT :


    def __init__(self) -> None:
        pass

    today = date.today()
    prevYear = today - timedelta(weeks=53)
    strf_today = datetime.strftime(today, '%Y-%m-%d')
    strf_prevYear = datetime.strftime(prevYear, '%Y-%m-%d')
    dbEngine = 'postgresql+psycopg2://postgres:s@unAsh*w3r@localhost/market'

    @classmethod
    def steep(cls, in_dbTable, out_dbTable, stISOf=strf_prevYear, endISOf=strf_today):
        engine = create_engine(cls.dbEngine)
        distinctTickers_query = '''
        SELECT DISTINCT "{ticker_col}"
        FROM "{in_dbTable}"
        WHERE "date" BETWEEN '{stISOf}' AND '{endISOf}' "
        ORDER BY "{ticker_col}" ASC;
        '''.format( 
            ticker_col='ticker',
            in_dbTable=in_dbTable,
            stISOf=stISOf,
            endISOf=endISOf
        )
        ticker_list = pd.read_sql_query(
            sql=distinctTickers_query,
            con=engine
        )
        loopCount = 0
        for ticker in ticker_list:
            loopCount += 1
            applyT_sql = '''
            SELECT *
            FROM "{in_dbTable}"
            WHERE "{ticker_col}"
            LIKE '{ticker}'
            AND "date" BETWEEN '{stISOf}' AND '{endISOf}'
            ORDER BY "date" ASC;
            '''.format(
                in_dbTable=in_dbTable,
                ticker_col='ticker',
                ticker=ticker,
                stISOf=stISOf,
                endISOf=endISOf
            )
            ticker_df = pd.read_sql_query(
                sql=applyT_sql,
                con=engine
            )
            wT_df = dailyStack(ticker_df, ticker)
            wT_df.to_sql(
                name=out_dbTable,
                con=engine,
                index=False,
                if_exists='append'
            )
            print(
                '\n'
                f'{loopCount} of {len(ticker_list)}: '
                f'{ticker} >> {out_dbTable}'
            )
        print(f'afternoonT.steep() complete!!!')


class pandascan:

    
    def __init__(self) -> None:
        pass


    @classmethod
    def pct48_KsubD_bbPw_RSI14pct24(cls, df):
        positive_idx = df.loc[
            (df['close_chngpct_48'] <= -0.35)
            & (df['pctK_sub_pctD'] >= 0.10)
            & (df['bb_power'] >= -0.05)
            & (df['RSI14_chngpct_24'] <= -3.22)
        ].index
        return(positive_idx)
