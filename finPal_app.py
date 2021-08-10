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


class afternoonT :


    def __init__(self) -> None:
        pass


    today = date.today()
    prevYear = today - timedelta(weeks=53)
    strf_today = datetime.strftime(today, '%Y-%m-%d')
    strf_prevYear = datetime.strftime(prevYear, '%Y-%m-%d')
    dbEngine = 'postgresql+psycopg2://postgres:s@unAsh*w3r@localhost/market'

    @classmethod
    def _pm_stack(cls, df, ticker) -> DataFrame:
        ''' one ticker at a time; order by date ascending '''
        # // close and vwap
        df['close_chngpct_24'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['close_chngpct_48'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['close_chngpct_72'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3)
        ).mul(100)
        df['close_chngpct_24_tmro'] = df['close_chngpct_24'].shift(-1)
        df['close_chngpct_24_yday'] = df['close_chngpct_24'].shift(1)
        df['close_chngpct_48_yday'] = df['close_chngpct_48'].shift(1)

        df['close_down3'] = 0
        df.loc[
            (df['close'] < df['close'].shift(1))
            & (df['close'].shift(1) < df['close'].shift(2)),
            ['close_down3']
        ] = 1

        df['perf_1wk'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=5)
        ).mul(100)
        df['perf_1wk_sub_pchngpct_24'] = (
            df['perf_1wk'].sub(
                (
                    df['close_chngpct_24'].where(
                        cond=df['close_chngpct_24'].gt(0)
                    )
                )
            )
        )
        df['perf_1wk_sub_nchngpct_24'] = (
            df['perf_1wk'].sub(
                ( 
                    df['close_chngpct_24'].where(
                        cond=df['close_chngpct_24'].le(0)
                    )
                )
            )
        )
        df['close_chngpct_1wk_high'] = (
            ( 
                df['close'].sub( 
                    (df['close'].rolling(window=5).max())
                )
            ).div( 
                df['close'].rolling(window=5).max()
            )
        ).mul(100)
        df['close_24_sub_p_pctf_1wk_high'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_chngpct_1wk_high'].where( 
                        cond=df['close_chngpct_1wk_high'].ge(0)
                    )
                )
            )
        )
        df['close_24_sub_n_pctf_1wk_high'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_chngpct_1wk_high'].where( 
                        cond=df['close_chngpct_1wk_high'].lt(0)
                    )
                )
            )
        )
        df['close_chngpct_1wk_low'] = (
            ( 
                df['close'].sub( 
                    (df['close'].rolling(window=5).min())
                )
            ).div( 
                df['close'].rolling(window=5).min()
            )
        ).mul(100)
        df['close_24_sub_p_pctf_1wk_low'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_chngpct_1wk_low'].where( 
                        cond=df['close_chngpct_1wk_low'].gt(0)
                    )
                )
            )
        )
        df['close_24_sub_n_pctf_1wk_low'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_chngpct_1wk_low'].where( 
                        cond=df['close_chngpct_1wk_low'].le(0)
                    )
                )
            )
        )
        df['perf_1mo'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=20).mul(100)
        )
        df['close_chngpct_1mo_high'] = (
            ( 
                df['close'].sub( 
                    (df['close'].rolling(window=20).max())
                )
            ).div( 
                df['close'].rolling(window=20).max()
            )
        ).mul(100)
        df['close_chngpct_1mo_low'] = (
            ( 
                df['close'].sub( 
                    (df['close'].rolling(window=20).min())
                )
            ).div( 
                df['close'].rolling(window=20).min()
            )
        ).mul(100)

        df['vwap_chngpct_24'] = (
            df['vwap'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1).mul(100)
        )
        df['vwap_chngpct_48'] = (
            df['vwap'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2).mul(100)
        )
        df['vwap_chngpct_72'] = (
            df['vwap'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3).mul(100)
        )
        df['vwap_chngpct_24_yday'] = df['vwap_chngpct_24'].shift(1)
        df['vwap_chngpct_48_yday'] = df['vwap_chngpct_48'].shift(1)

        # // volume
        df['volume_chngpct_24'] = (
            df['volume'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1).mul(100)
        )
        df['volume_1wk_high'] = df['volume'].rolling(window=5).max()
        df['volume_1wk_low'] = df['volume'].rolling(window=5).min()
        df['volume_chngpct_vhigh'] = (
            ( 
                df['volume'].sub( 
                    df['volume_1wk_high']
                )
            ).div( 
                df['volume_1wk_high']
            )
        ).mul(100)
        df['volume_chngpct_vlow'] = (
            ( 
                df['volume'].sub( 
                    df['volume_1wk_low']
                )
            ).div( 
                df['volume_1wk_low']
            )
        ).mul(100)
        df['close_at_vhigh_1wk'] = (
            df['close'].where(
                df['volume'] == df['volume_1wk_high']
            ).fillna(method='ffill')
        )
        df['close_at_vlow_1wk'] = (
            df['close'].where(
                df['volume'] == df['volume_1wk_low']
            ).fillna(method='ffill')
        )
        df['close_pctf_vhigh_1wk'] = (
            ( 
                df['close'].sub(
                    df['close_at_vhigh_1wk']
                )
            ).div(
                df['close_at_vhigh_1wk']
            )
        ).mul(100)
        df['close_pctf_vlow_1wk'] = (
            ( 
                df['close'].sub(
                    df['close_at_vlow_1wk']
                )
            ).div(
                df['close_at_vlow_1wk']
            )
        ).mul(100)

        df['avgVol_10d'] = df['volume'].rolling(window=10).mean()
        df['rvol'] = df['volume'].div(df['avgVol_10d'])
        df['avgVol_10d_chngpct_24'] = (
            df['avgVol_10d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['avgVol_10d_chngpct_48'] = (
            df['avgVol_10d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['avgVol_10d_chngpct_72'] = (
            df['avgVol_10d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3)
        ).mul(100)
        df['avgVol_30d'] = df['volume'].rolling(window=30).mean()
        df['avgVol_30d_chngpct_24'] = (
            df['avgVol_30d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['avgVol_30d_chngpct_48'] = (
            df['avgVol_30d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['avgVol_30d_chngpct_72'] = (
            df['avgVol_30d'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3)
        ).mul(100)
        df['avgVol_10d_pctf_av30d'] = (
            ( 
                df['avgVol_10d'].sub(
                    df['avgVol_30d']
                )
            ).div(
                df['avgVol_30d']
            )
        ).mul(100)

        df['volume_by_price'] = df['volume'] * df['vwap']

        df['WOBV'] = TA.WOBV(df)
        df['WOBV_chngpct_24'] = (
            df['WOBV'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['WOBV_chngpct_48'] = (
            df['WOBV'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['WOBV_chngpct_72'] = (
            df['WOBV'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3)
        ).mul(100)

        df['EFI'] = TA.EFI(df)
        df['EFI_diff_p_24'] = ( 
            df['EFI'].sub( 
                (
                    (df['EFI'].shift(1)).where( 
                        cond=df['EFI'].shift(1).gt(0)
                    )
                )
            )
        )
        df['EFI_diff_n_24'] = ( 
            df['EFI'].sub( 
                (
                    (df['EFI'].shift(1)).where( 
                        cond=df['EFI'].shift(1).le(0)
                    )
                )
            )
        )

        # // candles

        df['gap'] = (
            ( 
                df['open'].sub( 
                    df['close'].shift(1)
                )
            ).div( 
                df['close'].shift(1)
            )
        ).mul(100)

        df['bop'] = (
            (
                df['close'].sub(
                    df['open']
                )
            ).div(
                (
                    df['high'].sub(
                        df['low']
                    )
                ).replace(
                    to_replace=0, value=0.001
                )
            )
        )
        df['bop_diff_p_24'] = ( 
            df['bop'].sub( 
                (
                    (df['bop'].shift(1)).where( 
                        cond=df['bop'].shift(1).gt(0)
                    )
                )
            )
        )
        df['bop_diff_n_24'] = ( 
            df['bop'].sub( 
                (
                    (df['bop'].shift(1)).where( 
                        cond=df['bop'].shift(1).le(0)
                    )
                )
            )
        )
        df['bop_at_vhigh_1wk'] = ( 
            df['bop'].where( 
                df['volume'] == df['volume_1wk_high']
            ).fillna(method='ffill')
        )
        df['bop_sub_p_vhigh_1wk'] = ( 
            df['bop'].sub( 
                (
                    (df['bop_at_vhigh_1wk']).where( 
                        cond=df['bop_at_vhigh_1wk'].gt(0)
                    )
                )
            )
        )
        df['bop_sub_n_vhigh_1wk'] = ( 
            df['bop'].sub( 
                (
                    (df['bop_at_vhigh_1wk']).where( 
                        cond=df['bop_at_vhigh_1wk'].le(0)
                    )
                )
            )
        )
        df['bop_at_vlow_1wk'] = ( 
            df['bop'].where( 
                df['volume'] == df['volume_1wk_low']
            ).fillna(method='ffill')
        )
        df['bop_sub_p_vlow_1wk']= ( 
            df['bop'].sub( 
                (
                    (df['bop_at_vlow_1wk']).where( 
                        cond=df['bop_at_vlow_1wk'].gt(0)
                    )
                )
            )
        )
        df['bop_sub_n_vlow_1wk']= ( 
            df['bop'].sub( 
                (
                    (df['bop_at_vlow_1wk']).where( 
                        cond=df['bop_at_vlow_1wk'].le(0)
                    )
                )
            )
        )
        df['bop_SMA3'] = TA.SMA(df, period=3, column='bop')
        df['bop_EMA3'] = TA.EMA(df, period=3, column='bop')
        df['bop_week'] = (
            (
                df['close'].sub(
                    df['open']
                )
            ).div(
                (
                    (df['high'].rolling(window=5).max()).sub(
                        (df['low'].rolling(window=5).min())
                    )
                ).replace(
                    to_replace=0, value=0.001
                )
            )
        )

        BBP_df = TA.EBBP(df)
        BBP_df.rename(
            columns={'Bull.':'bull_power', 'Bear.':'bear_power'}, inplace=True
        )
        df = pd.concat([df, BBP_df], axis=1)
        df['bullPwr_diff_p_24'] = ( 
            df['bull_power'].sub( 
                (
                    (df['bull_power'].shift(1)).where( 
                        cond=df['bull_power'].shift(1).gt(0)
                    )
                )
            )
        )
        df['bullPwr_diff_n_24'] = ( 
            df['bull_power'].sub( 
                (
                    (df['bull_power'].shift(1)).where( 
                        cond=df['bull_power'].shift(1).le(0)
                    )
                )
            )
        )
        df['bearPwr_diff_p_24'] = ( 
            df['bear_power'].sub( 
                (
                    (df['bear_power'].shift(1)).where( 
                        cond=df['bear_power'].shift(1).gt(0)
                    )
                )
            )
        )
        df['bearPwr_diff_n_24'] = ( 
            df['bear_power'].sub( 
                (
                    (df['bear_power'].shift(1)).where( 
                        cond=df['bear_power'].shift(1).le(0)
                    )
                )
            )
        )
        df['bullPwr_sub_p_bearPwr'] = ( 
            df['bull_power'].sub( 
                (
                    (df['bear_power'].shift(1)).where( 
                        cond=df['bear_power'].gt(0)
                    )
                )
            )
        )
        df['bullPwr_sub_n_bearPwr'] = ( 
            df['bull_power'].sub( 
                (
                    (df['bear_power'].shift(1)).where( 
                        cond=df['bear_power'].le(0)
                    )
                )
            )
        )

        BASPN_df = TA.BASPN(df)
        BASPN_df.rename( 
            columns={'Buy.':'normBP', 'Sell.':'normSP'}, inplace=True
        )
        df = pd.concat([df, BASPN_df], axis=1)
        df['normBP_chngpct_24'] = (
            df['normBP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['normBP_chngpct_48'] = (
            df['normBP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['normSP_chngpct_24'] = (
            df['normSP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['normSP_chngpct_48'] = (
            df['normSP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['normBP_pctf_normSP'] = (
            ( 
                df['normBP'].sub(
                    df['normSP']
                )
            ).div(
                df['normSP']
            )
        ).mul(100)
        df['normBP_div_normSP'] = ( 
            df['normBP'].div( 
                df['normSP']
            )
        )
        df['normBP_div_normSP_chngpct_24'] = (
            df['normBP_div_normSP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['normBP_div_normSP_chngpct_48'] = (
            df['normBP_div_normSP'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        df['MOM5'] = TA.MOM(df, period=5, column='close')
        df['MOM10'] = TA.MOM(df, period=10, column='close')
        df['MOM5_sub_p_MOM10'] = (
            df['MOM5'].sub(
                (
                    df['MOM10'].where(
                        cond=df['MOM10'].gt(0)
                    )
                )
            )
        )
        df['MOM5_sub_n_MOM10'] = (
            df['MOM5'].sub(
                (
                    df['MOM10'].where(
                        cond=df['MOM10'].le(0)
                    )
                )
            )
        )
        
        # // oscillators

        vortex_df = TA.VORTEX(df, period=14)
        df = pd.concat([df, vortex_df], axis=1)
        df['VIp_chngpct_24'] = (
            df['VIp'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['VIp_chngpct_48'] = (
            df['VIp'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['VIm_chngpct_24'] = (
            df['VIm'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['VIm_chngpct_48'] = (
            df['VIm'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['VIp_pctf_VIm'] = (
            ( 
                df['VIp'].sub(
                    df['VIm']
                )
            ).div(
                df['VIm']
            )
        ).mul(100)
        df['VIp_div_VIm'] = ( 
            df['VIp'].div( 
                df['VIm']
            )
        )
        df['VIp_div_VIm_chngpct_24'] = (
            df['VIp_div_VIm'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['VIp_div_VIm_chngpct_48'] = (
            df['VIp_div_VIm'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        

        df['Williams_R'] = TA.WILLIAMS(df, period=14)
        df['Will_R_chngpct_24'] = (
            df['Williams_R'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['Will_R_chngpct_48'] = (
            df['Williams_R'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        df['UO'] = TA.UO(df)
        df['UO_chngpct_24'] = (
            df['UO'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['UO_chngpct_48'] = (
            df['UO'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        df['AO'] = TA.AO(df)
        df['AO_chngpct_24'] = (
            df['AO'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['AO_chngpct_48'] = (
            df['AO'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        df[['KST', 'KST_Sig']] = TA.KST(df)
        df['KST_chngpct_24'] = (
            df['KST'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['KST_chngpct_48'] = (
            df['KST'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['KST_Sig_chngpct_24'] = (
            df['KST_Sig'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['KST_Sig_chngpct_48'] = (
            df['KST_Sig'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['KST_sub_p_KST_Sig'] = (
            df['KST'].sub(
                (
                    df['KST_Sig'].where(
                        cond=df['KST_Sig'].gt(0)
                    )
                )
            )
        )
        df['KST_sub_n_KST_Sig'] = (
            df['KST'].sub(
                (
                    df['KST_Sig'].where(
                        cond=df['KST_Sig'].le(0)
                    )
                )
            )
        )

        df[['MACD', 'MACD_Sig']] = TA.MACD(df)
        df['MACD_chngpct_24'] = (
            df['MACD'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['MACD_Sig_chngpct_24'] = (
            df['MACD_Sig'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['MACD_sub_pMACD_Sig'] = ( 
            df['MACD'].sub( 
                (
                    (df['MACD_Sig']).where( 
                        cond=df['MACD_Sig'].gt(0)
                    )
                )
            )
        )
        df['MACD_sub_pMACD_Sig_p_diff'] = ( 
            df['MACD_sub_pMACD_Sig'].sub( 
                (
                    (df['MACD_sub_pMACD_Sig'].shift(1)).where( 
                        cond=df['MACD_sub_pMACD_Sig'].shift(1).gt(0)
                    )
                )
            )
        )
        df['MACD_sub_pMACD_Sig_n_diff'] = ( 
            df['MACD_sub_pMACD_Sig'].sub( 
                (
                    (df['MACD_sub_pMACD_Sig'].shift(1)).where( 
                        cond=df['MACD_sub_pMACD_Sig'].shift(1).le(0)
                    )
                )
            )
        )
        df['MACD_sub_nMACD_Sig'] = ( 
            df['MACD'].sub( 
                (
                    (df['MACD_Sig']).where( 
                        cond=df['MACD_Sig'].le(0)
                    )
                )
            )
        )
        df['MACD_sub_nMACD_Sig_p_diff'] = ( 
            df['MACD_sub_nMACD_Sig'].sub( 
                (
                    (df['MACD_sub_nMACD_Sig'].shift(1)).where( 
                        cond=df['MACD_sub_nMACD_Sig'].shift(1).gt(0)
                    )
                )
            )
        )
        df['MACD_sub_nMACD_Sig_n_diff'] = ( 
            df['MACD_sub_nMACD_Sig'].sub( 
                (
                    (df['MACD_sub_nMACD_Sig'].shift(1)).where( 
                        cond=df['MACD_sub_nMACD_Sig'].shift(1).le(0)
                    )
                )
            )
        )

        df[['PPO', 'PPO_Sig', 'PPO_histo']] = TA.PPO(df)
        df['PPO_chngpct_24'] = (
            df['PPO'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['PPO_Sig_chngpct_24'] = (
            df['PPO_Sig'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['PPO_sub_p_PPO_Sig'] = (
            df['PPO'].sub(
                (
                    df['PPO_Sig'].where(
                        cond=df['PPO_Sig'].gt(0)
                    )
                )
            )
        )
        df['PPO_sub_n_PPO_Sig'] = (
            df['PPO'].sub(
                (
                    df['PPO_Sig'].where(
                        cond=df['PPO_Sig'].le(0)
                    )
                )
            )
        )
        df['PPO_histo_p_diff'] = ( 
            df['PPO_histo'].sub( 
                (
                    (df['PPO_histo'].shift(1)).where( 
                        cond=df['PPO_histo'].shift(1).gt(0)
                    )
                )
            )
        )
        df['PPO_histo_n_diff'] = ( 
            df['PPO_histo'].sub( 
                (
                    (df['PPO_histo'].shift(1)).where( 
                        cond=df['PPO_histo'].shift(1).le(0)
                    )
                )
            )
        )

        df['CCI'] = TA.CCI(df)
        df['CCI_diff_p_24'] = ( 
            df['CCI'].sub( 
                (
                    (df['CCI'].shift(1)).where( 
                        cond=df['CCI'].shift(1).gt(0)
                    )
                )
            )
        )
        df['CCI_diff_n_24'] = ( 
            df['CCI'].sub( 
                (
                    (df['CCI'].shift(1)).where( 
                        cond=df['CCI'].shift(1).le(0)
                    )
                )
            )
        )

        df['RSI7'] = TA.RSI(df, period=7)
        df['RSI7_chngpct_24'] = (
            df['RSI7'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['RSI7_chngpct_48'] = (
            df['RSI7'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['RSI14'] = TA.RSI(df)
        df['RSI14_chngpct_24'] = (
            df['RSI14'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['RSI14_chngpct_48'] = (
            df['RSI14'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['RSI7_pctf_RSI14'] = (
            ( 
                df['RSI7'].sub(
                    df['RSI14']
                )
            ).div(
                df['RSI14']
            )
        ).mul(100)
        df['RSI7_div_RSI14'] = ( 
            df['RSI7'].div( 
                df['RSI14']
            )
        )
        df['RSI7_div_RSI14_chngpct_24'] = (
            df['RSI7_div_RSI14'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['RSI7_div_RSI14_chngpct_48'] = (
            df['RSI7_div_RSI14'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        df['pctK'] = TA.STOCH(df)
        df['pctK_chngpct_24'] = (
            df['pctK'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['pctK_chngpct_48'] = (
            df['pctK'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['pctD'] = TA.STOCHD(df)
        df['pctD_chngpct_24'] = (
            df['pctD'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['pctD_chngpct_48'] = (
            df['pctD'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['pctK_pctf_pctD'] = (
            ( 
                df['pctK'].sub(
                    df['pctD']
                )
            ).div(
                df['pctD']
            )
        ).mul(100)
        df['pctK_div_pctD'] = ( 
            df['pctK'].div( 
                df['pctD']
            )
        )
        df['pctK_div_pctD_chngpct_24'] = (
            df['pctK_div_pctD'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['pctK_div_pctD_chngpct_48'] = (
            df['pctK_div_pctD'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)

        # // trendlines

        fib_piv_df = TA.PIVOT_FIB(df)
        fib_piv_df.rename(
            columns={ 
                'pivot':'fib_pivot', 
                's1':'fib_s1', 
                's2':'fib_s2', 
                's3':'fib_s3', 
                's4':'fib_s4', 
                'r1':'fib_r1', 
                'r2':'fib_r2', 
                'r3':'fib_r3', 
                'r4':'fib_r4'
            },
            inplace=True
        )
        df = pd.concat([df, fib_piv_df], axis=1)
        df['close_pctf_fib_s3'] = (
            ( 
                df['close'].sub(
                    df['fib_s3']
                )
            ).div(
                df['fib_s3']
            )
        ).mul(100)
        df['close_pctf_fib_pivot'] = (
            ( 
                df['close'].sub(
                    df['fib_pivot']
                )
            ).div(
                df['fib_pivot']
            )
        ).mul(100)
        df['close_pctf_fib_r3'] = (
            ( 
                df['close'].sub(
                    df['fib_r3']
                )
            ).div(
                df['fib_r3']
            )
        ).mul(100)

        df['true_range'] = TA.TR(df)

        df['ATR14'] = TA.ATR(df)

        BB_df= TA.BBANDS(df, period=20)
        BB_df.rename(
            columns={ 
                'BB_UPPER':'BB_up', 
                'BB_MIDDLE':'BB_mid', 
                'BB_LOWER':'BB_low'
            }, 
            inplace=True
        )
        df = pd.concat([df, BB_df], axis=1)
        df['BB_width'] = TA.BBWIDTH(df, period=20)
        df['BB_pct_B'] = TA.PERCENT_B(df, period=20)
        df['close_pctf_BB_up'] = (
            ( 
                df['close'].sub(
                    df['BB_up']
                )
            ).div(
                df['BB_up']
            )
        ).mul(100)
        df['close_pctf_BB_low'] = (
            ( 
                df['close'].sub(
                    df['BB_low']
                )
            ).div(
                df['BB_low']
            )
        ).mul(100)

        df['EMA5'] = TA.EMA(df, period=5, column='close')

        df['close_pctf_EMA5'] = (
            ( 
                df['close'].sub(
                    df['EMA5']
                )
            ).div(
                df['EMA5']
            )
        ).mul(100)

        df['EMA10'] = TA.EMA(df, period=10, column='close')

        df['close_pctf_EMA10'] = (
            ( 
                df['close'].sub(
                    df['EMA10']
                )
            ).div(
                df['EMA10']
            )
        ).mul(100)

        df['EMA5_pctf_EMA10'] = (
            ( 
                df['EMA5'].sub(
                    df['EMA10']
                )
            ).div(
                df['EMA10']
            )
        ).mul(100)

        df['EMA30'] = TA.EMA(df, period=30, column='close')

        df['close_pctf_EMA30'] = (
            ( 
                df['close'].sub(
                    df['EMA30']
                )
            ).div(
                df['EMA30']
            )
        ).mul(100)

        df['EMA5_pctf_EMA30'] = (
            ( 
                df['EMA5'].sub(
                    df['EMA30']
                )
            ).div(
                df['EMA30']
            )
        ).mul(100)

        df['EMA50'] = TA.EMA(df, period=50, column='close')

        df['close_pctf_EMA50'] = (
            ( 
                df['close'].sub(
                    df['EMA50']
                )
            ).div(
                df['EMA50']
            )
        ).mul(100)

        df['EMA10_pctf_EMA50'] = (
            ( 
                df['EMA10'].sub(
                    df['EMA50']
                )
            ).div(
                df['EMA50']
            )
        ).mul(100)

        df['EMA100'] = TA.EMA(df, period=100, column='close')

        df['close_pctf_EMA100'] = (
            ( 
                df['close'].sub(
                    df['EMA50']
                )
            ).div(
                df['EMA50']
            )
        ).mul(100)

        df['EMA10_pctf_EMA100'] = (
            ( 
                df['EMA10'].sub(
                    df['EMA100']
                )
            ).div(
                df['EMA100']
            )
        ).mul(100)

        df['EMA50_pctf_EMA100'] = (
            ( 
                df['EMA50'].sub(
                    df['EMA100']
                )
            ).div(
                df['EMA100']
            )
        ).mul(100)

        df['SMA5'] = TA.SMA(df, period=5, column='close')

        df['close_pctf_SMA5'] = (
            ( 
                df['close'].sub(
                    df['SMA5']
                )
            ).div(
                df['SMA5']
            )
        ).mul(100)

        df['SMA10'] = TA.SMA(df, period=10, column='close')

        df['close_pctf_SMA10'] = (
            ( 
                df['close'].sub(
                    df['SMA10']
                )
            ).div(
                df['SMA10']
            )
        ).mul(100)

        df['SMA5_pctf_SMA10'] = (
            ( 
                df['SMA5'].sub(
                    df['SMA10']
                )
            ).div(
                df['SMA10']
            )
        ).mul(100)

        df['SMA30'] = TA.SMA(df, period=30, column='close')

        df['close_pctf_SMA30'] = (
            ( 
                df['close'].sub(
                    df['SMA30']
                )
            ).div(
                df['SMA30']
            )
        ).mul(100)

        df['SMA5_pctf_SMA30'] = (
            ( 
                df['SMA5'].sub(
                    df['SMA30']
                )
            ).div(
                df['SMA30']
            )
        ).mul(100)

        df['SMA50'] = TA.SMA(df, period=50, column='close')

        df['close_pctf_SMA50'] = (
            ( 
                df['close'].sub(
                    df['SMA50']
                )
            ).div(
                df['SMA50']
            )
        ).mul(100)

        df['SMA10_pctf_SMA50'] = (
            ( 
                df['SMA10'].sub(
                    df['SMA50']
                )
            ).div(
                df['SMA50']
            )
        ).mul(100)

        df['SMA100'] = TA.SMA(df, period=100, column='close')

        df['close_pctf_SMA100'] = (
            ( 
                df['close'].sub(
                    df['SMA50']
                )
            ).div(
                df['SMA50']
            )
        ).mul(100)

        df['SMA10_pctf_SMA100'] = (
            ( 
                df['SMA10'].sub(
                    df['SMA100']
                )
            ).div(
                df['SMA100']
            )
        ).mul(100)

        df['SMA50_pctf_SMA100'] = (
            ( 
                df['SMA50'].sub(
                    df['SMA100']
                )
            ).div(
                df['SMA100']
            )
        ).mul(100)

        df['SMA10_plus_TR'] = df['SMA10'].add(df['true_range'])
        df['close_pctf_TRonSMA10']= (
            ( 
                df['close'].sub(
                    df['SMA10_plus_TR']
                )
            ).div(
                df['SMA10_plus_TR']
            )
        ).mul(100)
        df['SMA10_plus_ATR'] = df['SMA10'].add(df['ATR14'])
        df['close_pctf_ATRonSMA10']= (
            ( 
                df['close'].sub(
                    df['SMA10_plus_ATR']
                )
            ).div(
                df['SMA10_plus_ATR']
            )
        ).mul(100)
        return df

    def steep(cls, in_dbTable, out_dbTable, stISOf=strf_prevYear, endISOf=strf_today):
        engine = create_engine(cls.dbEngine)
        distinctTickers_query = '''
        SELECT DISTINCT "{ticker_col}"
        FROM "{in_dbTable}"
        WHERE "date" BETWEEN '{stISOf}' AND '{endISOf}'
        ORDER BY "{ticker_col}" ASC;
        '''.format( 
            ticker_col='ticker',
            in_dbTable=in_dbTable,
            stISOf=stISOf,
            endISOf=endISOf
        )
        tickers_df = pd.read_sql_query(
            sql=distinctTickers_query,
            con=engine
        )
        ticker_list = tickers_df['ticker'].to_list()
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
            wT_df = cls._pm_stack(df=ticker_df, ticker=ticker)
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
