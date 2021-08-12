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
from pandas.core.frame import DataFrame
from pandas.core.indexes import period
import psycopg2
import requests
from sqlalchemy import create_engine
from finta import TA

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


class polygotter : 


    def __init__(self) -> None:
        pass


    _pwd = '/Users/clay/programming/finPal/finPal-app'
    today = date.today()
    strf_today = datetime.strftime(today, '%Y-%m-%d')
    url_base = 'https://api.polygon.io'
    apiKey = 'QLkG8Ff7Kupvv_M2h1RZxfrXzpMpoPdx'
    dbEngine = 'postgresql+psycopg2://postgres:s@unAsh*w3r@localhost/market'


    @classmethod
    def _strfdateArray(cls, stISOf, endISOf=strf_today) -> list:
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
    def gd_daterange_cs(cls, to_dbTable, stISOf, endISOf):
        ''' stISOf and endISOf take "YYYY-MM-DD" as strings. '''
        date_array = cls._strfdateArray(stISOf=stISOf, endISOf=endISOf)
        for adate in date_array:
            gd_df = cls.groupedDaily(gdDate=adate)
            ref_df = cls.refTickerType(refTickerDate=adate)
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
            gd_df['date'] = adate
            engine = (create_engine(cls.dbEngine))
            gd_df.to_sql( 
                name=to_dbTable,
                con=engine,
                index=False,
                if_exists='append'
            )
            print(f'gdCS >> {to_dbTable}')


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





class pandascan:


    def __init__(self) -> None:
        pass

    _filter_presets = []

    blip_array = []


    def what4(cls, df, wa, wb, wc, wd):
        ''' w_ should be ['tech', 'qleft', 'qright'] '''
        df.loc[ 
            (df[wa[0].between(left=df[wa[0]].quantile(wa[1]), right=df[wa[0]].quantile(wa[2]))])
            & (df[wb[0].between(left=df[wb[0]].quantile(wb[1]), right=df[wb[0]].quantile(wb[2]))])
            & (df[wc[0].between(left=df[wc[0]].quantile(wc[1]), right=df[wc[0]].quantile(wc[2]))])
            & (df[wd[0].between(left=df[wd[0]].quantile(wd[1]), right=df[wd[0]].quantile(wd[2]))])
        ]
        pass

    def bop__pctK_div_pctD_chngpct_48__SMA10_plus_ATR__close_24_sub_n_pctf_1wk_low(cls, df) -> None:
        ''' 0.943 | 416/441 | mo @ august 7 '''
        recent_Ov0c = 0.913
        positive_idx = df.loc[
            (
                (df['bop'].between(
                    left=df['bop'].quantile(0.00), right=df['bop'].quantile(0.50))
                )
                & (df['pctK_div_pctD_chngpct_48'].between(
                    left=df['pctK_div_pctD_chngpct_48'].quantile(0.50), right=df['pctK_div_pctD_chngpct_48'].quantile(1.00))
                )
                & (df['SMA10_plus_ATR'].between(
                    left=df['SMA10_plus_ATR'].quantile(0.50), right=df['SMA10_plus_ATR'].quantile(1.00))
                )
                & (df['close_24_sub_n_pctf_1wk_low'].between(
                    left=df['close_24_sub_n_pctf_1wk_low'].quantile(0.25), right=df['close_24_sub_n_pctf_1wk_low'].quantile(0.75))
                )
            )
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'bop_volume_vwap_close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
        )
    
    def bop__vwap__pctK_div_pctD_chngpct_48__close_24_sub_n_pctf_1wk_low(cls, df) -> None:
        ''' 0.945 | 397/420 | mo @ august 7 '''
        recent_Ov0c = 0.913
        positive_idx = df.loc[
            (
                (df['bop'].between(
                    left=df['bop'].quantile(0.00), right=df['bop'].quantile(0.50))
                )
                & (df['vwap'].between(
                    left=df['vwap'].quantile(0.50), right=df['vwap'].quantile(1.00))
                )
                & (df['pctK_div_pctD_chngpct_48'].between(
                    left=df['pctK_div_pctD_chngpct_48'].quantile(0.50), right=df['pctK_div_pctD_chngpct_48'].quantile(1.00))
                )
                & (df['close_24_sub_n_pctf_1wk_low'].between(
                    left=df['close_24_sub_n_pctf_1wk_low'].quantile(0.25), right=df['close_24_sub_n_pctf_1wk_low'].quantile(0.75))
                )
            )
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'bop_volume_vwap_close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
        )

    def bop_volume_vwap_close_24_sub_n_pctf_1wk_low(cls, df) -> None:
        ''' 0.913 | 336/368 | mo @ august 7 '''
        recent_Ov0c = 0.913
        positive_idx = df.loc[
            (
                (df['bop'].between(left=df['bop'].quantile(0.00), right=df['bop'].quantile(0.50)))
                & (df['volume'].between(left=df['volume'].quantile(0.25), right=df['volume'].quantile(0.75)))
                & (df['vwap'].between(left=df['vwap'].quantile(0.50), right=df['vwap'].quantile(1.00)))
                & (df['close_24_sub_n_pctf_1wk_low'].between(left=df['close_24_sub_n_pctf_1wk_low'].quantile(0.25), right=df['close_24_sub_n_pctf_1wk_low'].quantile(0.75)))
            )
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'bop_volume_vwap_close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
        )

    def bop_volume_transactions_close_24_sub_n_pctf_1wk_low(cls, df) -> None:
        ''' 0.873 | 428/490 | mo @ august 7 '''
        recent_Ov0c = 0.913
        positive_idx = df.loc[
            (
                (df['bop'].between(left=df['bop'].quantile(0.00), right=df['bop'].quantile(0.50)))
                & (df['volume'].between(left=df['volume'].quantile(0.25), right=df['volume'].quantile(0.75)))
                & (df['transactions'].between(left=df['transactions'].quantile(0.50), right=df['transactions'].quantile(1.00)))
                & (df['close_24_sub_n_pctf_1wk_low'].between(left=df['close_24_sub_n_pctf_1wk_low'].quantile(0.25), right=df['close_24_sub_n_pctf_1wk_low'].quantile(0.75)))
            )
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'bop_volume_vwap_close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
        )


    def pct48_KsubD_bbPw_RSI14pct24(cls, df) -> None:
        ''' recent_Ov0c = 1234 '''
        recent_Ov0c = 1234
        positive_idx = df.loc[
            (df['close_chngpct_48'] <= -0.35)
            & (df['pctK_sub_pctD'] >= 0.10)
            & (df['bb_power'] >= -0.05)
            & (df['RSI14_chngpct_24'] <= -3.22)
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'pct48_KsubD_bbPw_RSI14pct24', len(positive_idx), positive_idx]
        )


    def return_blip_df(cls) -> DataFrame :
        ''' access blips in ohlcv via df.loc[blip_df.at(i, 'blips_idx')] '''
        blip_df = pd.DataFrame(data=cls.blip_array, columns=['recent_Ov0c', 'filters', 'blips', 'blips_idx'])
        return blip_df


    def Ov0c_of(cls, df, Ov0_cutoff, test_idx, test_name) -> list:
        ''' return can be nested and framed '''
        obsv = len(df.loc[test_idx])
        Ov0 = len(df.loc[test_idx].loc['close_chngpct_24_tmro'] > Ov0_cutoff)
        Ov0c = Ov0 / obsv
        return [test_name, Ov0_cutoff, obsv, Ov0, Ov0c]
