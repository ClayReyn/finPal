
import time

import numpy as np
import pandas as pd
from finta import TA


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


#>
def chngpct_3day(df, quantVar):
    df[f'{quantVar}_chngpct_24'] = df[f'{quantVar}'].replace(to_replace=0, value=0.001).pct_change(periods=1).mul(100)
    df[f'{quantVar}_chngpct_48'] = df[f'{quantVar}'].replace(to_replace=0, value=0.001).pct_change(periods=2).mul(100)
    df[f'{quantVar}_chngpct_72'] = df[f'{quantVar}'].replace(to_replace=0, value=0.001).pct_change(periods=3).mul(100)

    df[f'{quantVar}_chngpct_24_yday'] = df[f'{quantVar}_chngpct_24'].shift(1)
    df[f'{quantVar}_chngpct_48_yday'] = df[f'{quantVar}_chngpct_24'].shift(2)
    
    df[f'{quantVar}_chngpct_24_2db4'] = df[f'{quantVar}_chngpct_24'].shift(3)

    return df


#>
def dailyStack(df, ticker):
    ''' load one ticker at a time, order by "date" ascending '''
    print( 
        f"afternoon_stack({ticker}) starting..."
        f" @ {time.strftime('%H:%M:%S', time.localtime())}"
    )

    df = chngpct_3day(df, 'close')
    df['close_chngpct_24_diff_p'] = df['close_chngpct_24'].sub((df['close_chngpct_24_yday'].where(cond=df['close_chngpct_24_yday'].gt(0))))
    df['close_chngpct_24_diff_n'] = df['close_chngpct_24'].sub((df['close_chngpct_24_yday'].where(cond=df['close_chngpct_24_yday'].le(0))))
    df['close_chngpct_24_tmro'] = df['close_chngpct_24'].shift(-1)
    df['close_chngpct_48_tmro'] = df['close_chngpct_24'].shift(-2)

    df['close_down3'] = 0
    df.loc[(df['close'] < df['close'].shift(1)) & (df['close'].shift(1) < df['close'].shift(2)), ['close_down3']] = 1

    =    

#>
def tradingview_stack(df, ticker):
    ''' load one ticker at a time, order by "date" ascending '''
    print( 
        f"appendTi({ticker}) starting..."
        f" @ {time.strftime('%H:%M:%S', time.localtime())}"
    )

    df = chngpct_3day(df, 'close')
    df['close_chngpct_24_diff_p'] = df['close_chngpct_24'].sub((df['close_chngpct_24_yday'].where(cond=df['close_chngpct_24_yday'].gt(0))))
    df['close_chngpct_24_diff_n'] = df['close_chngpct_24'].sub((df['close_chngpct_24_yday'].where(cond=df['close_chngpct_24_yday'].le(0))))
    df['close_chngpct_24_tmro'] = df['close_chngpct_24'].shift(-1)
    df['close_chngpct_48_tmro'] = df['close_chngpct_24'].shift(-1)
    
    df['close_down3'] = 0
    df.loc[(df['close'] < df['close'].shift(1)) & (df['close'].shift(1) < df['close'].shift(2)), ['close_down3']] = 1

    df['perf_1wk'] = df['close'].replace(to_replace=0, value=0.001).pct_change(periods=5).mul(100)
    df['close_1wk_low'] = df['close'].rolling(window=5).min()
    df['close_1wk_high'] = df['close'].rolling(window=5).max()
    df['close_div_1wk_low'] = df['close'].div(df['close_1wk_low'])
    df['high_1wk_div_close'] = df['close_1wk_high'].div(df['close'])

    df['perf_1wk_sub_pchngpct_24'] = df['perf_1wk'].sub((df['close_chngpct_24'].where(cond=df['close_chngpct_24'].gt(0))))
    df['perf_1wk_sub_nchngpct_24'] = df['perf_1wk'].sub((df['close_chngpct_24'].where(cond=df['close_chngpct_24'].le(0))))

    df['perf_1mo'] = df['close'].replace(to_replace=0, value=0.001).pct_change(periods=20).mul(100)
    df['close_1mo_low'] = df['close'].rolling(window=20).min()
    df['close_1mo_high'] = df['close'].rolling(window=20).max()
    df['close_div_1mo_low'] = df['close'].div(df['close_1mo_low'])
    df['high_1mo_div_close'] = df['close_1mo_high'].div(df['close'])

    df['perf_1mo_sub_p1wk'] = df['perf_1mo'].sub((df['perf_1wk'].where(cond=df['perf_1wk'].gt(0))))
    df['perf_1mo_sub_n1wk'] = df['perf_1mo'].sub((df['perf_1wk'].where(cond=df['perf_1wk'].le(0))))

    
    df['close_3mo_low'] = df['close'].rolling(window=60).min()
    df['close_3mo_high'] = df['close'].rolling(window=60).max()

    df['perf_3mo_sub_p1mo'] = df['perf_3mo'].sub((df['perf_1mo'].where(cond=df['perf_1mo'].gt(0))))
    df['perf_3mo_sub_n1mo'] = df['perf_3mo'].sub((df['perf_1mo'].where(cond=df['perf_1mo'].le(0))))

    # df['perf_52wk'] = df['close'].replace(to_replace=0, value=0.001).pct_change(periods=250).mul(100)
    # df['close_52wk_low'] = df['close'].rolling(window=250).min()
    # df['close_52wk_high'] = df['close'].rolling(window=250).max()

    # df['perf_6mo'] = df['close'].replace(to_replace=0, value=0.001).pct_change(periods=120).mul(100)
    # df['close_6mo_low'] = df['close'].rolling(window=120).min()
    # df['close_6mo_high'] = df['close'].rolling(window=120).max()

    #

    df = chngpct_3day(df, 'volume')
    df['volume_yday'] = df['volume'].shift(1)

    df['volume_1wk_low'] = df['volume'].rolling(window=5).min()
    df['volume_1wk_high'] = df['volume'].rolling(window=5).max()
    df['volume_div_1wk_low'] = df['volume'].div(df['volume_1wk_low'])
    df['high_1wk_div_volume'] = df['volume_1wk_high'].div(df['volume'])
    
    df['close_at_vol_1wk_high'] = df['close'].where(df['volume'] == df['volume_1wk_high']).fillna(method='ffill')
    df['close_at_vol_1wk_low'] = df['close'].where(df['volume'] == df['volume_1wk_low']).fillna(method='ffill')
    df['close_cpct_v_1wk_high'] = (df['close'].sub(df['close_at_vol_1wk_high'])).div(df['close_at_vol_1wk_high']) * 100
    df['close_cpct_v_1wk_low'] = (df['close'].sub(df['close_at_vol_1wk_low'])).div(df['close_at_vol_1wk_low']) * 100

    df['volume_1mo_low'] = df['volume'].rolling(window=20).min()
    df['volume_1mo_high'] = df['volume'].rolling(window=20).max()
    df['volume_div_1mo_low'] = df['volume'].div(df['volume_1mo_low'])
    df['high_1mo_div_volume'] = df['volume_1mo_high'].div(df['volume'])
    
    df['close_at_vol_1mo_high'] = df['close'].where(df['volume'] == df['volume_1mo_high']).fillna(method='ffill')
    df['close_at_vol_1mo_low'] = df['close'].where(df['volume'] == df['volume_1mo_low']).fillna(method='ffill')
    df['close_cpct_v_1mo_high'] = (df['close'].sub(df['close_at_vol_1mo_high'])).div(df['close_at_vol_1mo_high']) * 100
    df['close_cpct_v_1mo_low'] = (df['close'].sub(df['close_at_vol_1mo_low'])).div(df['close_at_vol_1mo_low']) * 100
 
    # df['volume_diff_p'] = df['volume'].sub(df['volume_yday'].where(cond=df['volume_yday'].gt(0)))
    # df['volume_diff_n'] = df['volume'].sub(df['volume_yday'].where(cond=df['volume_yday'].le(0)))
    # df['volume_div_p'] = df['volume'].div(df['volume_yday'].where(cond=df['volume_yday'].gt(0)))
    # df['volume_div_n'] = df['volume'].div(df['volume_yday'].where(cond=df['volume_yday'].le(0)))

    #

    df = chngpct_3day(df, 'vwap')

    #

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

    df['close_div_fib_s3'] = df['close'].div((df['fib_s3'].replace(0, 0.001)))
    df['close_div_fib_pivot'] = df['close'].div((df['fib_pivot'].replace(0, 0.001)))
    df['close_div_fib_r3'] = df['close'].div((df['fib_r3'].replace(0, 0.001)))

    df['true_range'] = TA.TR(df)

    df['ATR14'] = TA.ATR(df, period=14)
    period = 14
    # df.loc[0:(period-1), ['ATR14']] = np.NaN
    df = chngpct_3day(df, 'ATR14')

    df['gap'] = (
        (
            (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
        ) * 100
    )

    df['dollarchng'] = df['close'].diff(periods=1)

    # df['ATR14_div_pdollarchng'] = df['ATR14'].div((df['dollarchng'].gt(0)))
    # df['ATR14_div_ndollarchng'] = df['ATR14'].div((df['dollarchng'].lt(0)))

    df['VWMA'] = df['vwap'].rolling(window=3).mean()
    df = chngpct_3day(df, 'VWMA')

    df['volume_by_price'] = df['volume'] * df['vwap']

    df['avgVol_10day'] = df['volume'].rolling(window=10).mean()

    df['avgVol_30day'] = df['volume'].rolling(window=30).mean()

    df['avgVol_60day'] = df['volume'].rolling(window=60).mean()

    # df['avgVol_90day'] = df['volume'].rolling(window=90).mean()

    df = chngpct_3day(df, 'avgVol_10day')
    df['avgVol_10d_div_30d'] = df['avgVol_10day'] / df['avgVol_30day']

    df['rvol'] = df['volume'].div(df['avgVol_10day'])

    df['ADX'] = TA.ADX(df, period=14)
    # period = 14
    # df.loc[0:(period-1), ['ADX']] = np.NaN
    df = chngpct_3day(df, 'ADX')

    df['AO'] = TA.AO(df, slow_period=34, fast_period=5)
    slow_period = 34
    # df.loc[0:(slow_period-1), ['AO']] = np.NaN
    df = chngpct_3day(df, 'AO')

    BB_df= TA.BBANDS(df, period=20)
    period = 20
    # BB_df.loc[0:(period-1), ['BB_UPPER', 'BB_MIDDLE', 'BB_LOWER']] = np.NaN
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
    period = 20
    # df.loc[0:(period-1), ['BB_width']] = np.NaN
    df['BB_pct_B'] = TA.PERCENT_B(df, period=20)
    period = 20
    # df.loc[0:(period-1), ['BB_pct_B']] = np.NaN
    df['close_div_BB_up'] = df['close'].div((df['BB_up'].replace(0, 0.001)))
    df['close_div_BB_low'] = df['close'].div((df['BB_low'].replace(0, 0.001)))

    BBP_df = TA.EBBP(df)
    period = 13
    # BBP_df.loc[0:period-1, ['Bull.', 'Bear.']] = np.NaN
    BBP_df.rename(
        columns={'Bull.':'bull_power', 'Bear.':'bear_power'}, inplace=True
    )
    df = pd.concat([df, BBP_df], axis=1)
    df['bull_sub_pbear'] = df['bull_power'].sub((df['bear_power']).where(df['bear_power'].gt(0)))
    df['bull_sub_nbear'] = df['bull_power'].sub((df['bear_power']).where(df['bear_power'].le(0)))
    df['bull_div_pbear'] = df['bull_power'].div((df['bear_power'].where(df['bear_power'].gt(0))))
    df['bull_div_nbear'] = df['bull_power'].div((df['bear_power'].where(df['bear_power'].lt(0))))

    = #repace 0 to avoid inf

    df['bb_power'] = (df['close'].sub(df['open'])).div((df['high'].sub(df['low']).replace(0, 0.001)))
    df = chngpct_3day(df, 'bb_power')
    df['bb_power_yday'] = df['bb_power'].shift(1)
    df['bbPw_at_v_1wk_high'] = df['bb_power'].where(df['volume'] == df['volume_1wk_high']).fillna(method='ffill')
    df['bbPw_at_v_1wk_low'] = df['bb_power'].where(df['volume'] == df['volume_1wk_low']).fillna(method='ffill')
    df['bbPw_cpct_v_1wk_high'] = (df['bb_power'].sub(df['bbPw_at_v_1wk_high'])).div(df['bbPw_at_v_1wk_high']) * 100
    df['bbPw_cpct_v_1wk_low'] = (df['bb_power'].sub(df['bbPw_at_v_1wk_low'])).div(df['bbPw_at_v_1wk_low']) * 100
    df['bbPw_at_v_1mo_high'] = df['bb_power'].where(df['volume'] == df['volume_1mo_high']).fillna(method='ffill')
    df['bbPw_at_v_1mo_low'] = df['bb_power'].where(df['volume'] == df['volume_1mo_low']).fillna(method='ffill')
    df['bbPw_cpct_v_1mo_high'] = (df['bb_power'].sub(df['bbPw_at_v_1mo_high'])).div(df['bbPw_at_v_1mo_high']) * 100
    df['bbPw_cpct_v_1mo_low'] = (df['bb_power'].sub(df['bbPw_at_v_1mo_low'])).div(df['bbPw_at_v_1mo_low']) * 100
    
    df['bbPw_at_1wk_high'] = df['bb_power'].where(df['close'] == df['close_1wk_high']).fillna(method='ffill')
    df['bbPw_at_1wk_low'] = df['bb_power'].where(df['close'] == df['close_1wk_low']).fillna(method='ffill')
    df['bbPw_cpct_1wk_high'] = (df['bb_power'].sub(df['bbPw_at_1wk_high'])).div(df['bbPw_at_1wk_high']) * 100
    df['bbPw_cpct_1wk_low'] = (df['bb_power'].sub(df['bbPw_at_1wk_low'])).div(df['bbPw_at_1wk_low']) * 100
    df['bbPw_at_1mo_high'] = df['bb_power'].where(df['close'] == df['close_1mo_high']).fillna(method='ffill')
    df['bbPw_at_1mo_low'] = df['bb_power'].where(df['close'] == df['close_1mo_low']).fillna(method='ffill')
    df['bbPw_cpct_1mo_high'] = (df['bb_power'].sub(df['bbPw_at_1mo_high'])).div(df['bbPw_at_1mo_high']) * 100
    df['bbPw_cpct_1mo_low'] = (df['bb_power'].sub(df['bbPw_at_1mo_low'])).div(df['bbPw_at_1mo_low']) * 100

    df['bb_power_1wk'] = (df['close'].sub(df['open'])).div((df['close_1wk_high'].sub(df['close_1wk_low']).replace(0, 0.001)))
    df['bb_power_1mo'] = (df['close'].sub(df['open'])).div((df['close_1mo_high'].sub(df['close_1mo_low']).replace(0, 0.001)))

    BASPN_df = TA.BASPN(df, period=40)
    BASPN_df.rename( 
        columns={'nbf':'norm_BPressure', 'nsf':'norm_SPressure'}, inplace=True
    )
    pd.concat([df, BASPN_df], axis=1)

    df['CCI'] = TA.CCI(df)
    # period = 20
    # df.loc[0:period - 1, ['CCI']] = np.NaN
    df = chngpct_3day(df, 'CCI')

    df['EMA5'] = TA.EMA(df, period=5, column='close')
    # period = 5
    # df.loc[0:(period-1), ['EMA5']] = np.NaN

    df['EMA10'] = TA.EMA(df, period=10, column='close')
    # period = 10
    # df.loc[0:(period-1), ['EMA10']] = np.NaN

    df['EMA5_div_EMA10'] = df['EMA5'].div(df['EMA10'])

    df['EMA30'] = TA.EMA(df, period=30, column='close')
    # period = 30
    # df.loc[0:(period-1), ['EMA30']] = np.NaN

    df['EMA10_div_EMA30'] = df['EMA10'].div(df['EMA30'])
    df['EMA5ov10_div_EMA10ov30'] = df['EMA5_div_EMA10'].div(df['EMA10_div_EMA30'])

    df['EMA50'] = TA.EMA(df, period=50, column='close')
    # period = 50
    # df.loc[0:(period-1), ['EMA50']] = np.NaN

    df['EMA30_div_EMA50'] = df['EMA30'].div(df['EMA50'])
    df['EMA5ov10_div_EMA30ov50'] = df['EMA10_div_EMA30'].div(df['EMA30_div_EMA50'])

    df['EMA100'] = TA.EMA(df, period=100, column='close')
    # period = 100
    # df.loc[0:(period-1), ['EMA100']] = np.NaN

    df['EMA50_div_EMA100'] = df['EMA50'].div(df['EMA100'])
    df['EMA5ov10_div_EMA50ov100'] = df['EMA5_div_EMA10'].div(df['EMA50_div_EMA100'])

    df['EMA200'] = TA.EMA(df, period=200, column='close')
    # period = 200
    # df.loc[0:(period-1), ['EMA200']] = np.NaN

    df['EMA100_div_EMA200'] = df['EMA100'].div(df['EMA200'])
    df['EMA5ov10_div_EMA100ov200'] = df['EMA5_div_EMA10'].div(df['EMA100_div_EMA200'])

    df['SMA5'] = TA.SMA(df, period=5, column='close')
    # period = 5
    # df.loc[0:(period-1), ['SMA5']] = np.NaN

    df['SMA10'] = TA.SMA(df, period=10, column='close')
    # period = 10
    # df.loc[0:(period-1), ['SMA10']] = np.NaN

    df['SMA5_div_SMA10'] = df['SMA5'].div(df['SMA10'])

    df['SMA30'] = TA.SMA(df, period=30, column='close')
    # period = 30
    # df.loc[0:(period-1), ['SMA30']] = np.NaN

    df['SMA10_div_SMA30'] = df['SMA10'].div(df['SMA30'])
    df['SMA5ov10_div_SMA10ov30'] = df['SMA5_div_SMA10'].div(df['SMA10_div_SMA30'])

    df['SMA50'] = TA.SMA(df, period=50, column='close')
    # period = 50
    # df.loc[0:(period-1), ['SMA50']] = np.NaN

    df['SMA30_div_SMA50'] = df['SMA30'].div(df['SMA50'])
    df['SMA5ov10_div_SMA30ov50'] = df['SMA10_div_SMA30'].div(df['SMA30_div_SMA50'])

    df['SMA100'] = TA.SMA(df, period=100, column='close')
    # period = 100
    # df.loc[0:(period-1), ['SMA100']] = np.NaN

    df['SMA50_div_SMA100'] = df['SMA50'].div(df['SMA100'])
    df['SMA5ov10_div_SMA50ov100'] = df['SMA5_div_SMA10'].div(df['SMA50_div_SMA100'])

    df['SMA200'] = TA.SMA(df, period=200, column='close')
    # period = 200
    # df.loc[0:(period-1), ['SMA200']] = np.NaN

    df['SMA100_div_SMA200'] = df['SMA100'].div(df['SMA200'])
    df['SMA5ov10_div_SMA100ov200'] = df['SMA5_div_SMA10'].div(df['SMA100_div_SMA200'])

    df['HMA9'] = TA.HMA(df, period=9)
    # period = 9
    # df.loc[0:(period-1), ['HMA9']] = np.NaN

    df['EMA5_div_HMA9'] = df['EMA5'].div(df['HMA9'])

    ichimoku_df = ( 
        TA.ICHIMOKU( 
            df, 
            tenkan_period=9, 
            kijun_period=26, 
            senkou_period=52, 
            chikou_period=26 
        )
    )
    senkou_period = 52
    ichimoku_df.loc[ 
        0:(senkou_period-1), 
        ['TENKAN', 'KIJUN', 'senkou_span_a', 'SENKOU', 'CHIKOU'] 
    ] = np.NaN
    ichimoku_df.rename(
        columns={ 
            'TENKAN':'tenkan', 
            'KIJUN':'kijun', 
            'SENKOU':'senkou_span_b', 
            'CHIKOU':'chikou' 
        }, 
        inplace=True
    )
    df = pd.concat([df, ichimoku_df], axis=1)

    df['tenkan_div_kijun'] = df['tenkan'].div(df['kijun'])
    df['tenkan_div_senkou_a'] = df['tenkan'].div(df['kijun'])
    df['tenkan_div_senkou_b'] = df['tenkan'].div(df['kijun'])
    df['tenkan_div_chikou'] = df['tenkan'].div(df['kijun'])

    df['kijun_div_senkou_a'] = df['tenkan'].div(df['kijun'])
    df['kijun_div_senkou_b'] = df['tenkan'].div(df['kijun'])
    df['kijun_div_chikou'] = df['tenkan'].div(df['kijun'])

    df['senkou_a_div_senkou_b'] = df['tenkan'].div(df['kijun'])
    df['senkou_div_chikou'] = df['tenkan'].div(df['kijun'])

    df['senkou_b_div_chikou'] = df['tenkan'].div(df['kijun'])

    df[['MACD', 'MACD_Signal']] = ( 
        TA.MACD(df, period_fast=12, period_slow=26, signal=9, column='close') 
    )
    period_slow = 26
    # df.loc[0:period_slow-1, ['MACD', 'MACD_Signal']] = np.NaN
    df['MACD_Signal'].replace(to_replace = 0, value=0.001, inplace=True)
    df['MACD_sub_pSignal'] = df['MACD'].sub((df['MACD_Signal'].where(cond=df['MACD_Signal'].gt(0))))
    df['MACD_sub_nSignal'] = df['MACD'].sub((df['MACD_Signal'].where(cond=df['MACD_Signal'].le(0))))
    df['MACD_div_pSignal'] = df['MACD'].div((df['MACD_Signal'].where(cond=df['MACD_Signal'].gt(0))))
    df['MACD_div_nSignal'] = df['MACD'].div((df['MACD_Signal'].where(cond=df['MACD_Signal'].lt(0))))

    df = chngpct_3day(df, 'MACD')
    df = chngpct_3day(df, 'MACD_Signal')
    # df = chngpct_3day(df, 'MACD_sub_pSignal')
    # df = chngpct_3day(df, 'MACD_sub_nSignal')
    # df = chngpct_3day(df, 'MACD_div_pSignal')
    # df = chngpct_3day(df, 'MACD_div_nSignal')

    df['MOM10'] = TA.MOM(df, period=10, column='close')
    # period = 10
    # df.loc[0:(period-1), ['MOM10']] = np.NaN
    df = chngpct_3day(df, 'MOM10')

    df['MFI'] = TA.MFI(df, period=14)
    # period = 14
    # df.loc[0:(period-1), ['MFI']] = np.NaN
    df = chngpct_3day(df, 'MFI')

    DMI_df = TA.DMI(df, period=14)
    # period = 14
    # DMI_df.loc[0:(period-1), ['DI+', 'DI-', 'DMI_plus', 'DMI_minus']] = np.NaN
    DMI_df.rename(
        columns={'DI+':'DI_plus', 'DI-':'DI_minus'}, inplace=True
    )
    df = pd.concat([df, DMI_df], axis=1)

    df['ROC9'] = TA.ROC(df, period=9)
    df = chngpct_3day(df, 'ROC9')

    df['ROC18'] = TA.ROC(df, period=18)
    df = chngpct_3day(df, 'ROC18')

    df['ROC36'] = TA.ROC(df, period=36)
    df = chngpct_3day(df, 'ROC36')

    df['ROC72'] = TA.ROC(df, period=72)
    df = chngpct_3day(df, 'ROC72')

    df['ROC9_div_ROC18'] = df['ROC9'].div(df['ROC18'].replace(0, 0.001))
    df['ROC9_div_ROC36'] = df['ROC9'].div(df['ROC36'].replace(0, 0.001))
    df['ROC9_div_ROC72'] = df['ROC9'].div(df['ROC72'].replace(0, 0.001))

    df['RSI7'] = TA.RSI(df, period=7, column='close')
    # period = 7
    # df.loc[0:(period-1), ['RSI7']] = np.NaN
    df = chngpct_3day(df, 'RSI7')

    df['RSI14'] = TA.RSI(df, period=14, column='close')
    # period = 14
    # df.loc[0:(period-1), ['RSI14']] = np.NaN
    df = chngpct_3day(df, 'RSI14')

    df['RSI7_sub_RSI14'] = df['RSI7'].sub(df['RSI14'])
    df = chngpct_3day(df, 'RSI7_sub_RSI14')

    df['RSI7_div_RSI14'] = df['RSI7'].div(df['RSI14'])
    df = chngpct_3day(df, 'RSI7_div_RSI14')

    df['pct_K'] = TA.STOCH(df, period=14)
    # period = 14
    # df.loc[0:(period-1), ['pct_K']] = np.NaN
    df = chngpct_3day(df, 'pct_K')

    df['pct_D'] = TA.STOCHD(df, period=3, stoch_period=14)
    # stoch_period = 14
    # df.loc[0:(stoch_period-1), ['pct_D']] = np.NaN
    df = chngpct_3day(df, 'pct_D')

    df['pctK_sub_pctD'] = df['pct_K'].sub(df['pct_D'])
    df = chngpct_3day(df, 'pctK_sub_pctD')
    df['pctK_div_pctD'] = df['pct_K'].div((df['pct_D'].replace(0, 0.001)))
    df = chngpct_3day(df, 'pctK_div_pctD')

    df['Stoch_RSI14'] = TA.STOCHRSI(df, rsi_period=14, stoch_period=14)
    # stoch_period = 14
    # df.loc[0:(stoch_period-1), ['Stoch_RSI']] = np.NaN
    df = chngpct_3day(df, 'Stoch_RSI14')

    df['UO'] = TA.UO(df)
    # period = 28
    # df.loc[0:(period-1), ['UO']] = np.NaN
    df = chngpct_3day(df, 'UO')

    df['Williams_R'] = TA.WILLIAMS(df, period=14)
    # stoch_period = 14
    # df.loc[0:(stoch_period-1), ['Williams_R']] = np.NaN
    df = chngpct_3day(df, 'Williams_R')

    vortex_df = TA.VORTEX(df, period=14)
    df = pd.concat([df, vortex_df], axis=1)
    df['VIp_sub_pVIm'] = df['VIp'].sub((df['VIm'].where(cond=df['VIm'].gt(0))))
    df['VIp_sub_nVIm'] = df['VIp'].sub((df['VIm'].where(cond=df['VIm'].le(0))))
    df['VIp_div_pVIm'] = df['VIp'].div((df['VIm'].where(cond=df['VIm'].gt(0))))
    df['VIp_div_nVIm'] = df['VIp'].div((df['VIm'].where(cond=df['VIm'].le(0))))
    df = chngpct_3day(df, 'VIp')
    df = chngpct_3day(df, 'VIm')

    df['parabolic_SAR'] =

    return df