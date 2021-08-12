import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from finta import TA
import psycopg2
from sqlalchemy import create_engine


class pmT :


    def __init__(self) -> None:
        pass


    today = date.today()
    prevYear = today - timedelta(weeks=53)
    strf_today = datetime.strftime(today, '%Y-%m-%d')
    strf_prevYear = datetime.strftime(prevYear, '%Y-%m-%d')
    dbEngine = 'postgresql+psycopg2://postgres:s@unAsh*w3r@localhost/market'

    _pm_stack_test_list = [ 
        'close_chngpct_24',
        'close_chngpct_48',
        'close_chngpct_72',
        'close_chngpct_24_yday',
        'close_chngpct_48_yday',
        'close_down3',
        'perf_1wk',
        'perf_1wk_sub_p_chngpct_24',
        'perf_1wk_sub_n_chngpct_24',
        'close_pctf_1wk_high',
        'close_24_sub_p_pctf_1wk_high',
        'close_24_sub_n_pctf_1wk_high',
        'close_pctf_1wk_low',
        'close_24_sub_p_pctf_1wk_low',
        'close_24_sub_n_pctf_1wk_low',
        'close_48_dvrg_perf_1wk',
        'perf_1wk_dvrg_close_48',
        'perf_1mo',
        'close_pctf_1mo_high',
        'close_pctf_1mo_low',
        'vwap',
        'vwap_chngpct_24',
        'vwap_chngpct_48',
        'vwap_chngpct_72',
        'vwap_chngpct_24_yday',
        'vwap_chngpct_48_yday',
        'volume_chngpct_24',
        'close_48_dvrg_v_24',
        'v_24_close_48',
        'volume_chngpct_48',
        'close_48_dvrg_v_48',
        'v_48_dvrg_close',
        'volume_1wk_high',
        'volume_1wk_low',
        'volume_pctf_vhigh',
        'volume_pctf_vlow',
        'close_pctf_vhigh_1wk',
        'close_pctf_vlow_1wk',
        'avgVol_10d',
        'rvol',
        'avgVol_10d_chngpct_24',
        'avgVol_10d_chngpct_48',
        'close_48_dvrg_avgv_10d_48',
        'avgv_10d_48_dvrg_close_48',
        'avgVol_10d_chngpct_72',
        'avgVol_30d',
        'avgVol_30d_chngpct_24',
        'avgVol_30d_chngpct_48',
        'avgVol_30d_chngpct_72',
        'avgVol_10d_pctf_av30d',
        'volume_by_price',
        'WOBV',
        'WOBV_chngpct_24',
        'WOBV_chngpct_48',
        'close_48_dvrg_WOBV_48',
        'WOBV_48_dvrg_close_48',
        'WOBV_chngpct_72',
        'EFI',
        'EFI_diff_p_24',
        'EFI_diff_n_24',
        'gap',
        'gap_sub_p_close_chngpct_24_yday',
        'gap_sub_n_close_chngpct_24_yday',
        'bop',
        'bop_diff_p_24',
        'bop_diff_n_24',
        'bop_at_vhigh_1wk',
        'bop_sub_p_vhigh_1wk',
        'bop_sub_n_vhigh_1wk',
        'bop_at_vlow_1wk',
        'bop_sub_p_vlow_1wk',
        'bop_sub_n_vlow_1wk',
        'bop_SMA3',
        'bop_EMA3',
        'bop_week',
        'bull_power',
        'bear_power',
        'bullPwr_diff_p_24',
        'bullPwr_diff_n_24',
        'bearPwr_diff_p_24',
        'bearPwr_diff_n_24',
        'bullPwr_sub_p_bearPwr',
        'bullPwr_sub_n_bearPwr',
        'normBP',
        'normSP',
        'normBP_chngpct_24',
        'normBP_chngpct_48',
        'close_48_dvrg_normBP_48',
        'normBP_48_dvrg_close_48',
        'normSP_chngpct_24',
        'normSP_chngpct_48',
        'close_48_dvrg_normSP_48',
        'normSP_48_dvrg_close_48',
        'normBP_pctf_normSP',
        'normBP_div_normSP',
        'normBP_div_normSP_chngpct_24',
        'normBP_div_normSP_chngpct_48',
        'MOM5',
        'MOM10',
        'MOM5_sub_p_MOM10',
        'MOM5_sub_n_MOM10',
        'VIp',
        'VIp_chngpct_24',
        'VIp_chngpct_48',
        'close_48_dvrg_VIp_48',
        'VIp_48_dvrg_close_48',
        'VIm',
        'VIm_chngpct_24',
        'VIm_chngpct_48',
        'close_48_dvrg_VIm_48',
        'VIm_48_dvrg_close_48',
        'VIp_pctf_VIm',
        'VIp_div_VIm',
        'VIp_div_VIm_chngpct_24',
        'VIp_div_VIm_chngpct_48',
        'VIp_xup_VIm',
        'VIm_xup_VIp',
        'Williams_R',
        'Will_R_chngpct_24',
        'Will_R_chngpct_48',
        'close_48_dvrg_WillR_48',
        'WillR_48_dvrg_close_48',
        'UO',
        'UO_chngpct_24',
        'UO_chngpct_48',
        'close_48_dvrg_UO_48',
        'UO_48_dvrg_close_48',
        'AO',
        'AO_chngpct_24',
        'AO_chngpct_48',
        'close_48_dvrg_AO_48',
        'AO_48_dvrg_close_48',
        'KST',
        'KST_Sig',
        'KST_chngpct_24',
        'KST_chngpct_48',
        'close_48_dvrg_KST_48',
        'KST_48_dvrg_close_48',
        'KST_Sig_chngpct_24',
        'KST_Sig_chngpct_48',
        'close_48_dvrg_KST_Sig_48',
        'KST_Sig_48_dvrg_close_48',
        'KST_sub_p_KST_Sig',
        'KST_sub_n_KST_Sig',
        'KST_xup_KST_Sig',
        'KST_Sig_xup_KST',
        'MACD',
        'MACD_Sig',
        'MACD_chngpct_24',
        'MACD_Sig_chngpct_24',
        'MACD_sub_pMACD_Sig',
        'MACD_sub_pMACD_Sig_p_diff',
        'MACD_sub_pMACD_Sig_n_diff',
        'MACD_sub_nMACD_Sig',
        'MACD_sub_nMACD_Sig_p_diff',
        'MACD_sub_nMACD_Sig_n_diff',
        'MACD_xup_MACD_Sig',
        'MACD_Sig_xup_MACD',
        'MACD_xup_0',
        '0_xup_MACD',
        'MACD_Sig_xup_0',
        '0_xup_MACD_Sig',
        'PPO',
        'PPO_Sig',
        'PPO_histo',
        'PPO_sub_p_PPO_Sig',
        'PPO_sub_n_PPO_Sig',
        'PPO_histo_p_diff',
        'PPO_histo_n_diff',
        'PPO_xup_PPO_Sig',
        'PPO_Sig_xup_PPO',
        'PPO_xup_0',
        '0_xup_PPO',
        'PPO_Sig_xup_0',
        '0_xup_PPO_Sig',
        'CCI',
        'CCI_diff_p_24',
        'CCI_diff_n_24',
        'CCI_xup_0',
        '0_xup_CCI',
        'RSI7',
        'RSI7_chngpct_24',
        'RSI7_chngpct_48',
        'close_48_dvrg_RSI7_48',
        'RSI7_48_dvrg_close_48',
        'RSI14',
        'RSI14_chngpct_24',
        'RSI14_chngpct_48',
        'close_48_dvrg_RSI14_48',
        'RSI14_48_dvrg_close_48',
        'RSI7_pctf_RSI14',
        'RSI7_div_RSI14',
        'RSI7_div_RSI14_chngpct_24',
        'RSI7_div_RSI14_chngpct_48',
        'RSI7_xup_RSI14',
        'RSI14_xup_RSI7',
        'pctK',
        'pctK_chngpct_24',
        'pctK_chngpct_48',
        'close_48_dvrg_pctK_48',
        'pctK_48_dvrg_close_48',
        'pctD',
        'pctD_chngpct_24',
        'pctD_chngpct_48',
        'close_48_dvrg_pctD_48',
        'pctD_48_dvrg_close_48',
        'pctK_pctf_pctD',
        'pctK_div_pctD',
        'pctK_div_pctD_chngpct_24',
        'pctK_div_pctD_chngpct_48',
        'pctK_xup_pctD',
        'pctD_xup_pctK',
        'close_pctf_fib_s3',
        'close_pctf_fib_pivot',
        'close_pctf_fib_r3',
        'BB_width',
        'BB_pct_B',
        'close_pctf_BB_up',
        'close_pctf_BB_low',
        'close_xup_BB_low',
        'BB_low_xup_close',
        'close_pctf_EMA5',
        'close_xup_EMA5',
        'EMA5_xup_close',
        'close_pctf_EMA10',
        'EMA5_pctf_EMA10',
        'close_pctf_EMA30',
        'EMA5_pctf_EMA30',
        'close_pctf_EMA50',
        'EMA10_pctf_EMA50',
        'close_pctf_EMA50',
        'EMA10_pctf_EMA50',
        'close_pctf_EMA100',
        'EMA10_pctf_EMA100',
        'EMA50_pctf_EMA100',
        'close_pctf_SMA5',
        'close_xup_SMA5',
        'SMA5_xup_close',
        'close_pctf_SMA10',
        'close_xup_SMA10',
        'SMA10_xup_close',
        'SMA5_pctf_SMA10',
        'close_pctf_SMA30',
        'SMA5_pctf_SMA30',
        'close_xup_SMA30',
        'SMA30_xup_close',
        'SMA10_xup_SMA30',
        'SMA30_xup_SMA10',
        'close_pctf_SMA50',
        'SMA10_xup_SMA50',
        'SMA50_xup_SMA10',
        'SMA30_xup_SMA50',
        'SMA50_xup_SMA30',
        'SMA10_pctf_SMA50',
        'close_pctf_SMA100',
        'SMA10_pctf_SMA100',
        'SMA50_pctf_SMA100',
        'close_pctf_ATRonSMA10',
        'close_xup_SMA10_plus_ATR',
        'SMA10_plus_ATR_xup_close',
        'close_pctf_ATRunSMA10',
        'close_xup_SMA10_sub_ATR',
        'SMA10_sub_ATR_xup_close',
    ]

    _pm_stack_quant_set = { 
        'close_chngpct_24',
        'close_chngpct_48',
        'close_chngpct_72',
        'close_chngpct_24_yday',
        'close_chngpct_48_yday',
        'perf_1wk',
        'perf_1wk_sub_p_chngpct_24',
        'perf_1wk_sub_n_chngpct_24',
        'close_pctf_1wk_high',
        'close_24_sub_p_pctf_1wk_high',
        'close_24_sub_n_pctf_1wk_high',
        'close_pctf_1wk_low',
        'close_24_sub_p_pctf_1wk_low',
        'close_24_sub_n_pctf_1wk_low',
        'perf_1mo',
        'close_pctf_1mo_high',
        'close_pctf_1mo_low',
        'vwap',
        'vwap_chngpct_24',
        'vwap_chngpct_48',
        'vwap_chngpct_72',
        'vwap_chngpct_24_yday',
        'vwap_chngpct_48_yday',
        'volume_chngpct_24',
        'volume_chngpct_48',
        'volume_1wk_high',
        'volume_1wk_low',
        'volume_pctf_vhigh',
        'volume_pctf_vlow',
        'close_pctf_vhigh_1wk',
        'close_pctf_vlow_1wk',
        'avgVol_10d',
        'rvol',
        'avgVol_10d_chngpct_24',
        'avgVol_10d_chngpct_48',
        'avgVol_10d_chngpct_72',
        'avgVol_30d',
        'avgVol_30d_chngpct_24',
        'avgVol_30d_chngpct_48',
        'avgVol_30d_chngpct_72',
        'avgVol_10d_pctf_av30d',
        'volume_by_price',
        'WOBV',
        'WOBV_chngpct_24',
        'WOBV_chngpct_48',
        'WOBV_chngpct_72',
        'EFI',
        'EFI_diff_p_24',
        'EFI_diff_n_24',
        'gap',
        'gap_sub_p_close_chngpct_24_yday',
        'gap_sub_n_close_chngpct_24_yday',
        'bop',
        'bop_diff_p_24',
        'bop_diff_n_24',
        'bop_at_vhigh_1wk',
        'bop_sub_p_vhigh_1wk',
        'bop_sub_n_vhigh_1wk',
        'bop_at_vlow_1wk',
        'bop_sub_p_vlow_1wk',
        'bop_sub_n_vlow_1wk',
        'bop_SMA3',
        'bop_EMA3',
        'bop_week',
        'bull_power',
        'bear_power',
        'bullPwr_diff_p_24',
        'bullPwr_diff_n_24',
        'bearPwr_diff_p_24',
        'bearPwr_diff_n_24',
        'bullPwr_sub_p_bearPwr',
        'bullPwr_sub_n_bearPwr',
        'normBP',
        'normSP',
        'normBP_chngpct_24',
        'normBP_chngpct_48',
        'normSP_chngpct_24',
        'normSP_chngpct_48',
        'normBP_pctf_normSP',
        'normBP_div_normSP',
        'normBP_div_normSP_chngpct_24',
        'normBP_div_normSP_chngpct_48',
        'MOM5',
        'MOM10',
        'MOM5_sub_p_MOM10',
        'MOM5_sub_n_MOM10',
        'VIp',
        'VIp_chngpct_24',
        'VIp_chngpct_48',
        'VIm',
        'VIm_chngpct_24',
        'VIm_chngpct_48',
        'VIp_pctf_VIm',
        'VIp_div_VIm',
        'VIp_div_VIm_chngpct_24',
        'VIp_div_VIm_chngpct_48',
        'Williams_R',
        'Will_R_chngpct_24',
        'Will_R_chngpct_48',
        'close_48_dvrg_WillR_48',
        'WillR_48_dvrg_close_48',
        'UO',
        'UO_chngpct_24',
        'UO_chngpct_48',
        'AO',
        'AO_chngpct_24',
        'AO_chngpct_48',
        'KST',
        'KST_Sig',
        'KST_chngpct_24',
        'KST_chngpct_48',
        'KST_Sig_chngpct_24',
        'KST_Sig_chngpct_48',
        'KST_sub_p_KST_Sig',
        'KST_sub_n_KST_Sig',
        'MACD',
        'MACD_Sig',
        'MACD_chngpct_24',
        'MACD_Sig_chngpct_24',
        'MACD_sub_pMACD_Sig',
        'MACD_sub_pMACD_Sig_p_diff',
        'MACD_sub_pMACD_Sig_n_diff',
        'MACD_sub_nMACD_Sig',
        'MACD_sub_nMACD_Sig_p_diff',
        'MACD_sub_nMACD_Sig_n_diff',
        'PPO',
        'PPO_Sig',
        'PPO_histo',
        'PPO_sub_p_PPO_Sig',
        'PPO_sub_n_PPO_Sig',
        'PPO_histo_p_diff',
        'PPO_histo_n_diff',
        'CCI',
        'CCI_diff_p_24',
        'CCI_diff_n_24',
        'RSI7',
        'RSI7_chngpct_24',
        'RSI7_chngpct_48',
        'RSI14',
        'RSI14_chngpct_24',
        'RSI14_chngpct_48',
        'RSI7_pctf_RSI14',
        'RSI7_div_RSI14',
        'RSI7_div_RSI14_chngpct_24',
        'RSI7_div_RSI14_chngpct_48',
        'pctK',
        'pctK_chngpct_24',
        'pctK_chngpct_48',
        'pctD',
        'pctD_chngpct_24',
        'pctD_chngpct_48',
        'pctK_pctf_pctD',
        'pctK_div_pctD',
        'pctK_div_pctD_chngpct_24',
        'pctK_div_pctD_chngpct_48',
        'close_pctf_fib_s3',
        'close_pctf_fib_pivot',
        'close_pctf_fib_r3',
        'BB_width',
        'BB_pct_B',
        'close_pctf_BB_up',
        'close_pctf_BB_low',
        'close_pctf_EMA5',
        'close_pctf_EMA10',
        'EMA5_pctf_EMA10',
        'close_pctf_EMA30',
        'EMA5_pctf_EMA30',
        'close_pctf_EMA50',
        'EMA10_pctf_EMA50',
        'close_pctf_EMA50',
        'EMA10_pctf_EMA50',
        'close_pctf_EMA100',
        'EMA10_pctf_EMA100',
        'EMA50_pctf_EMA100',
        'close_pctf_SMA5',
        'close_pctf_SMA10',
        'SMA5_pctf_SMA10',
        'close_pctf_SMA30',
        'SMA5_pctf_SMA30',
        'close_pctf_SMA50',
        'SMA10_pctf_SMA50',
        'close_pctf_SMA100',
        'SMA10_pctf_SMA100',
        'SMA50_pctf_SMA100',
        'close_pctf_ATRonSMA10',
        'close_pctf_ATRunSMA10'
    }

    _pm_stack_binary_set = {
        'close_down3',
        'close_48_dvrg_perf_1wk',
        'perf_1wk_dvrg_close_48',
        'close_48_dvrg_v_24',
        'v_24_close_48',
        'close_48_dvrg_v_48',
        'v_48_dvrg_close',
        'close_48_dvrg_avgv_10d_48',
        'avgv_10d_48_dvrg_close_48',
        'close_48_dvrg_WOBV_48',
        'WOBV_48_dvrg_close_48',
        'close_48_dvrg_normBP_48',
        'normBP_48_dvrg_close_48',
        'close_48_dvrg_normSP_48',
        'normSP_48_dvrg_close_48',
        'close_48_dvrg_VIp_48',
        'VIp_48_dvrg_close_48',
        'close_48_dvrg_VIm_48',
        'VIm_48_dvrg_close_48',
        'VIp_xup_VIm',
        'VIm_xup_VIp',
        'close_48_dvrg_UO_48',
        'UO_48_dvrg_close_48',
        'close_48_dvrg_AO_48',
        'AO_48_dvrg_close_48',
        'close_48_dvrg_KST_48',
        'KST_48_dvrg_close_48',
        'close_48_dvrg_KST_Sig_48',
        'KST_Sig_48_dvrg_close_48',
        'KST_xup_KST_Sig',
        'KST_Sig_xup_KST',
        'MACD_xup_MACD_Sig',
        'MACD_Sig_xup_MACD',
        'MACD_xup_0',
        '0_xup_MACD',
        'MACD_Sig_xup_0',
        '0_xup_MACD_Sig',
        'PPO_xup_PPO_Sig',
        'PPO_Sig_xup_PPO',
        'PPO_xup_0',
        '0_xup_PPO',
        'PPO_Sig_xup_0',
        '0_xup_PPO_Sig',
        'CCI_xup_0',
        '0_xup_CCI',
        'close_48_dvrg_RSI7_48',
        'RSI7_48_dvrg_close_48',
        'close_48_dvrg_RSI14_48',
        'RSI14_48_dvrg_close_48',
        'RSI7_xup_RSI14',
        'RSI14_xup_RSI7',
        'close_48_dvrg_pctK_48',
        'pctK_48_dvrg_close_48',
        'close_48_dvrg_pctD_48',
        'pctD_48_dvrg_close_48',
        'pctK_xup_pctD',
        'pctD_xup_pctK',
        'close_xup_BB_low',
        'BB_low_xup_close',
        'close_xup_EMA5',
        'EMA5_xup_close',
        'close_xup_SMA5',
        'SMA5_xup_close',
        'close_xup_SMA10',
        'SMA10_xup_close',
        'close_xup_SMA30',
        'SMA30_xup_close',
        'SMA10_xup_SMA30',
        'SMA30_xup_SMA10',
        'SMA10_xup_SMA50',
        'SMA50_xup_SMA10',
        'SMA30_xup_SMA50',
        'SMA50_xup_SMA30',
        'close_xup_SMA10_plus_ATR',
        'SMA10_plus_ATR_xup_close',
        'close_xup_SMA10_sub_ATR',
        'SMA10_sub_ATR_xup_close'
    }

    _pm_stack_radio_set = { 
        'UO',
        'AO',
        'RSI7',
        'RSI14',
        'pctK',
        'pctD'
    }

    @classmethod
    def _pm_stack(cls, df, ticker):
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
        df['perf_1wk_sub_p_chngpct_24'] = (
            df['perf_1wk'].sub(
                (
                    df['close_chngpct_24'].where(
                        cond=df['close_chngpct_24'].gt(0)
                    )
                )
            )
        )
        df['perf_1wk_sub_n_chngpct_24'] = (
            df['perf_1wk'].sub(
                ( 
                    df['close_chngpct_24'].where(
                        cond=df['close_chngpct_24'].le(0)
                    )
                )
            )
        )
        df['close_pctf_1wk_high'] = (
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
                    df['close_pctf_1wk_high'].where( 
                        cond=df['close_pctf_1wk_high'].ge(0)
                    )
                )
            )
        )
        df['close_24_sub_n_pctf_1wk_high'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_pctf_1wk_high'].where( 
                        cond=df['close_pctf_1wk_high'].lt(0)
                    )
                )
            )
        )
        df['close_pctf_1wk_low'] = (
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
                    df['close_pctf_1wk_low'].where( 
                        cond=df['close_pctf_1wk_low'].gt(0)
                    )
                )
            )
        )
        df['close_24_sub_n_pctf_1wk_low'] = ( 
            df['close_chngpct_24'].sub( 
                (
                    df['close_pctf_1wk_low'].where( 
                        cond=df['close_pctf_1wk_low'].le(0)
                    )
                )
            )
        )
        df['close_48_dvrg_perf_1wk'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['perf_1wk'] < 0.10
                )
            ), ['close_48_dvrg_perf_1wk']
        ] = 1
        df['perf_1wk_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['perf_1wk'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['perf_1wk_dvrg_close_48']
        ] = 1

        df['perf_1mo'] = (
            df['close'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=20).mul(100)
        )
        df['close_pctf_1mo_high'] = (
            ( 
                df['close'].sub( 
                    (df['close'].rolling(window=20).max())
                )
            ).div( 
                df['close'].rolling(window=20).max()
            )
        ).mul(100)
        df['close_pctf_1mo_low'] = (
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
            ).pct_change(periods=1)
        ).mul(100)
        df['vwap_chngpct_48'] = (
            df['vwap'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['vwap_chngpct_72'] = (
            df['vwap'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=3)
        ).mul(100)

        df['vwap_chngpct_24_yday'] = df['vwap_chngpct_24'].shift(1)
        df['vwap_chngpct_48_yday'] = df['vwap_chngpct_48'].shift(1)

        # // volume
        df['volume_chngpct_24'] = (
            df['volume'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=1)
        ).mul(100)
        df['close_48_dvrg_v_24'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['volume_chngpct_24'] < 0.10
                )
            ), ['close_48_dvrg_v_24']
        ] = 1
        df['v_24_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['volume_chngpct_24'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['v_24_dvrg_close_48']
        ] = 1
        df['volume_chngpct_48'] = (
            df['volume'].replace(
                to_replace=0, value=0.001
            ).pct_change(periods=2)
        ).mul(100)
        df['close_48_dvrg_v_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['volume_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_v_48']
        ] = 1
        df['v_48_dvrg_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['volume_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['v_48_dvrg_close']
        ] = 1

        df['volume_1wk_high'] = df['volume'].rolling(window=5).max()
        df['volume_1wk_low'] = df['volume'].rolling(window=5).min()
        df['volume_pctf_vhigh'] = (
            ( 
                df['volume'].sub( 
                    df['volume_1wk_high']
                )
            ).div( 
                df['volume_1wk_high']
            )
        ).mul(100)
        df['volume_pctf_vlow'] = (
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
        df['close_48_dvrg_avgv_10d_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['avgVol_10d_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_avgv_10d_48']
        ] = 1
        df['avgv_10d_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['avgVol_10d_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['avgv_10d_48_dvrg_close_48']
        ] = 1
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
        df['close_48_dvrg_WOBV_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['WOBV_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_WOBV_48']
        ] = 1
        df['WOBV_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['WOBV_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['WOBV_48_dvrg_close_48']
        ] = 1
        
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
        df['gap_sub_p_close_chngpct_24_yday'] = ( 
            df['gap'].sub( 
                df['close_chngpct_24_yday'].where( 
                    df['close_chngpct_24_yday'].gt(0)
                )
            )
        )
        df['gap_sub_n_close_chngpct_24_yday'] = ( 
            df['gap'].sub( 
                df['close_chngpct_24_yday'].where( 
                    df['close_chngpct_24_yday'].le(0)
                )
            )
        )

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
        df['close_48_dvrg_normBP_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['normBP_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_normBP_48']
        ] = 1
        df['normBP_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['normBP_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['normBP_48_dvrg_close_48']
        ] = 1
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
        df['close_48_dvrg_normSP_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['normSP_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_normSP_48']
        ] = 1
        df['normSP_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['normSP_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['normSP_48_dvrg_close_48']
        ] = 1
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
        df['close_48_dvrg_VIp_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['VIp_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_VIp_48']
        ] = 1
        df['VIp_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['VIp_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['VIp_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_VIm_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['VIm_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_VIm_48']
        ] = 1
        df['VIm_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['VIm_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['VIm_48_dvrg_close_48']
        ] = 1

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

        df['VIp_xup_VIm'] = 0
        df.loc[ 
            ( 
                ( 
                    df['VIp'] > df['VIm'] 
                ) & ( 
                    df['VIp'].shift(1) <= df['VIm'].shift(1)
                )
            ), ['VIp_xup_VIm']
        ] = 1
        df['VIm_xup_VIp'] = 0
        df.loc[ 
            ( 
                ( 
                    df['VIm'] > df['VIp'] 
                ) & ( 
                    df['VIm'].shift(1) <= df['VIp'].shift(1)
                )
            ), ['VIm_xup_VIp']
        ] = 1

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
        df['close_48_dvrg_WillR_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['Will_R_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_WillR_48']
        ] = 1
        df['WillR_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['Will_R_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['WillR_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_UO_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['UO_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_UO_48']
        ] = 1
        df['UO_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['UO_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['UO_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_AO_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['AO_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_AO_48']
        ] = 1
        df['AO_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['AO_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['AO_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_KST_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['KST_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_KST_48']
        ] = 1
        df['KST_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['KST_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['KST_48_dvrg_close_48']
        ] = 1
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
        df['close_48_dvrg_KST_Sig_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['KST_Sig_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_KST_Sig_48']
        ] = 1
        df['KST_Sig_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['KST_Sig_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['KST_Sig_48_dvrg_close_48']
        ] = 1
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

        df['KST_xup_KST_Sig'] = 0
        df.loc[ 
            ( 
                ( 
                    df['KST'] > df['KST_Sig'] 
                ) & ( 
                    df['KST'].shift(1) <= df['KST_Sig'].shift(1)
                )
            ), ['KST_xup_KST_Sig']
        ] = 1
        df['KST_Sig_xup_KST'] = 0
        df.loc[ 
            ( 
                ( 
                    df['KST_Sig'] > df['KST'] 
                ) & ( 
                    df['KST_Sig'].shift(1) <= df['KST'].shift(1)
                )
            ), ['KST_Sig_xup_KST']
        ] = 1

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

        df['MACD_xup_MACD_Sig'] = 0
        df.loc[ 
            ( 
                ( 
                    df['MACD'] > df['MACD_Sig'] 
                ) & ( 
                    df['MACD'].shift(1) <= df['MACD_Sig'].shift(1)
                )
            ), ['MACD_xup_MACD_Sig']
        ] = 1
        df['MACD_Sig_xup_MACD'] = 0
        df.loc[ 
            ( 
                ( 
                    df['MACD_Sig'] > df['MACD'] 
                ) & ( 
                    df['MACD_Sig'].shift(1) <= df['MACD'].shift(1)
                )
            ), ['MACD_Sig_xup_MACD']
        ] = 1

        df['MACD_xup_0'] = 0
        df.loc[ 
            ( 
                ( 
                    df['MACD'] > 0 
                ) & ( 
                    df['MACD'].shift(1) <= 0
                )
            ), ['MACD_xup_0']
        ] = 1
        df['0_xup_MACD'] = 0
        df.loc[ 
            ( 
                ( 
                    0 > df['MACD'] 
                ) & ( 
                    0 <= df['MACD'].shift(1)
                )
            ), ['0_xup_MACD']
        ] = 1

        df['MACD_Sig_xup_0'] = 0
        df.loc[ 
            ( 
                ( 
                    df['MACD_Sig'] > 0 
                ) & ( 
                    df['MACD_Sig'].shift(1) <= 0
                )
            ), ['MACD_Sig_xup_0']
        ] = 1
        df['0_xup_MACD_Sig'] = 0
        df.loc[ 
            ( 
                ( 
                    0 > df['MACD_Sig'] 
                ) & ( 
                    0 <= df['MACD_Sig'].shift(1)
                )
            ), ['0_xup_MACD_Sig']
        ] = 1

        df[['PPO', 'PPO_Sig', 'PPO_histo']] = TA.PPO(df)
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

        df['PPO_xup_PPO_Sig'] = 0
        df.loc[ 
            ( 
                ( 
                    df['PPO'] > df['PPO_Sig'] 
                ) & ( 
                    df['PPO'].shift(1) <= df['PPO_Sig'].shift(1)
                )
            ), ['PPO_xup_PPO_Sig']
        ] = 1
        df['PPO_Sig_xup_PPO'] = 0
        df.loc[ 
            ( 
                ( 
                    df['PPO_Sig'] > df['PPO'] 
                ) & ( 
                    df['PPO_Sig'].shift(1) <= df['PPO'].shift(1)
                )
            ), ['PPO_Sig_xup_PPO']
        ] = 1

        df['PPO_xup_0'] = 0
        df.loc[ 
            ( 
                ( 
                    df['PPO'] > 0 
                ) & ( 
                    df['PPO'].shift(1) <= 0
                )
            ), ['PPO_xup_0']
        ] = 1
        df['0_xup_PPO'] = 0
        df.loc[ 
            ( 
                ( 
                    0 > df['PPO'] 
                ) & ( 
                    0 <= df['PPO'].shift(1)
                )
            ), ['0_xup_PPO']
        ] = 1

        df['PPO_Sig_xup_0'] = 0
        df.loc[ 
            ( 
                ( 
                    df['PPO_Sig'] > 0 
                ) & ( 
                    df['PPO_Sig'].shift(1) <= 0
                )
            ), ['PPO_Sig_xup_0']
        ] = 1
        df['0_xup_PPO_Sig'] = 0
        df.loc[ 
            ( 
                ( 
                    0 > df['PPO_Sig'] 
                ) & ( 
                    0 <= df['PPO_Sig'].shift(1)
                )
            ), ['0_xup_PPO_Sig']
        ] = 1


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

        df['CCI_xup_0'] = 0
        df.loc[ 
            ( 
                ( 
                    df['CCI'] > 0 
                ) & ( 
                    df['CCI'].shift(1) <= 0
                )
            ), ['CCI_xup_0']
        ] = 1
        df['0_xup_CCI'] = 0
        df.loc[ 
            ( 
                ( 
                    0 > df['CCI'] 
                ) & ( 
                    0 <= df['CCI'].shift(1)
                )
            ), ['0_xup_CCI']
        ] = 1

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
        df['close_48_dvrg_RSI7_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['RSI7_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_RSI7_48']
        ] = 1
        df['RSI7_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['RSI7_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['RSI7_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_RSI14_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['RSI14_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_RSI14_48']
        ] = 1
        df['RSI14_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['RSI14_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['RSI14_48_dvrg_close_48']
        ] = 1

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

        df['RSI7_xup_RSI14'] = 0
        df.loc[ 
            ( 
                ( 
                    df['RSI7'] > df['RSI14'] 
                ) & ( 
                    df['RSI7'].shift(1) <= df['RSI14'].shift(1)
                )
            ), ['RSI7_xup_RSI14']
        ] = 1
        df['RSI14_xup_RSI7'] = 0
        df.loc[ 
            ( 
                ( 
                    df['RSI14'] > df['RSI7'] 
                ) & ( 
                    df['RSI14'].shift(1) <= df['RSI7'].shift(1)
                )
            ), ['RSI14_xup_RSI7']
        ] = 1

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
        df['close_48_dvrg_pctK_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['pctK_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_pctK_48']
        ] = 1
        df['pctK_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['pctK_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['pctK_48_dvrg_close_48']
        ] = 1

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
        df['close_48_dvrg_pctD_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close_chngpct_48'] > 0.10
                ) & ( 
                    df['pctD_chngpct_48'] < 0.10
                )
            ), ['close_48_dvrg_pctD_48']
        ] = 1
        df['pctD_48_dvrg_close_48'] = 0
        df.loc[ 
            ( 
                ( 
                    df['pctD_chngpct_48'] > 0.10
                ) & ( 
                    df['close_chngpct_48'] < 0.10
                )
            ), ['pctD_48_dvrg_close_48']
        ] = 1

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

        df['pctK_xup_pctD'] = 0
        df.loc[ 
            ( 
                ( 
                    df['pctK'] > df['pctD'] 
                ) & ( 
                    df['pctK'].shift(1) <= df['pctD'].shift(1)
                )
            ), ['pctK_xup_pctD']
        ] = 1
        df['pctD_xup_pctK'] = 0
        df.loc[ 
            ( 
                ( 
                    df['pctD'] > df['pctK'] 
                ) & ( 
                    df['pctD'].shift(1) <= df['pctK'].shift(1)
                )
            ), ['pctD_xup_pctK']
        ] = 1

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

        df['close_xup_BB_low'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['BB_low'] 
                ) & ( 
                    df['close'].shift(1) <= df['BB_low'].shift(1)
                )
            ), ['close_xup_BB_low']
        ] = 1
        df['BB_low_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['BB_low'] > df['close'] 
                ) & ( 
                    df['BB_low'].shift(1) <= df['close'].shift(1)
                )
            ), ['BB_low_xup_close']
        ] = 1

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

        df['close_xup_EMA5'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['EMA5'] 
                ) & ( 
                    df['close'].shift(1) <= df['EMA5'].shift(1)
                )
            ), ['close_xup_EMA5']
        ] = 1
        df['EMA5_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['EMA5'] > df['close'] 
                ) & ( 
                    df['EMA5'].shift(1) <= df['close'].shift(1)
                )
            ), ['EMA5_xup_close']
        ] = 1

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

        df['close_xup_SMA5'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['SMA5'] 
                ) & ( 
                    df['close'].shift(1) <= df['SMA5'].shift(1)
                )
            ), ['close_xup_SMA5']
        ] = 1
        df['SMA5_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA5'] > df['close'] 
                ) & ( 
                    df['SMA5'].shift(1) <= df['close'].shift(1)
                )
            ), ['SMA5_xup_close']
        ] = 1

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

        df['close_xup_SMA10'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['SMA10'] 
                ) & ( 
                    df['close'].shift(1) <= df['SMA10'].shift(1)
                )
            ), ['close_xup_SMA10']
        ] = 1
        df['SMA10_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA10'] > df['close'] 
                ) & ( 
                    df['SMA10'].shift(1) <= df['close'].shift(1)
                )
            ), ['SMA10_xup_close']
        ] = 1

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

        df['close_xup_SMA30'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['SMA30'] 
                ) & ( 
                    df['close'].shift(1) <= df['SMA30'].shift(1)
                )
            ), ['close_xup_SMA30']
        ] = 1
        df['SMA30_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA30'] > df['close'] 
                ) & ( 
                    df['SMA30'].shift(1) <= df['close'].shift(1)
                )
            ), ['SMA30_xup_close']
        ] = 1

        df['SMA10_xup_SMA30'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA10'] > df['SMA30'] 
                ) & ( 
                    df['SMA10'].shift(1) <= df['SMA30'].shift(1)
                )
            ), ['SMA10_xup_SMA30']
        ] = 1
        df['SMA30_xup_SMA10'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA30'] > df['SMA10'] 
                ) & ( 
                    df['SMA30'].shift(1) <= df['SMA10'].shift(1)
                )
            ), ['SMA30_xup_SMA10']
        ] = 1

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

        df['SMA10_xup_SMA50'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA10'] > df['SMA50'] 
                ) & ( 
                    df['SMA10'].shift(1) <= df['SMA50'].shift(1)
                )
            ), ['SMA10_xup_SMA50']
        ] = 1
        df['SMA50_xup_SMA10'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA50'] > df['SMA10'] 
                ) & ( 
                    df['SMA50'].shift(1) <= df['SMA10'].shift(1)
                )
            ), ['SMA50_xup_SMA10']
        ] = 1

        df['SMA30_xup_SMA50'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA30'] > df['SMA50'] 
                ) & ( 
                    df['SMA30'].shift(1) <= df['SMA50'].shift(1)
                )
            ), ['SMA30_xup_SMA50']
        ] = 1
        df['SMA50_xup_SMA30'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA50'] > df['SMA30'] 
                ) & ( 
                    df['SMA50'].shift(1) <= df['SMA30'].shift(1)
                )
            ), ['SMA50_xup_SMA30']
        ] = 1

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
        df['close_xup_SMA10_plus_ATR'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['SMA10_plus_ATR'] 
                ) & ( 
                    df['close'].shift(1) <= df['SMA10_plus_ATR'].shift(1)
                )
            ), ['close_xup_SMA10_plus_ATR']
        ] = 1
        df['SMA10_plus_ATR_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA10_plus_ATR'] > df['close'] 
                ) & ( 
                    df['SMA10_plus_ATR'].shift(1) <= df['close'].shift(1)
                )
            ), ['SMA10_plus_ATR_xup_close']
        ] = 1

        df['SMA10_sub_ATR'] = df['SMA10'].sub(df['ATR14'])
        df['close_pctf_ATRunSMA10']= (
            ( 
                df['close'].sub(
                    df['SMA10_sub_ATR']
                )
            ).div(
                df['SMA10_sub_ATR']
            )
        ).mul(100)
        df['close_xup_SMA10_sub_ATR'] = 0
        df.loc[ 
            ( 
                ( 
                    df['close'] > df['SMA10_sub_ATR'] 
                ) & ( 
                    df['close'].shift(1) <= df['SMA10_sub_ATR'].shift(1)
                )
            ), ['close_xup_SMA10_sub_ATR']
        ] = 1
        df['SMA10_sub_ATR_xup_close'] = 0
        df.loc[ 
            ( 
                ( 
                    df['SMA10_sub_ATR'] > df['close'] 
                ) & ( 
                    df['SMA10_sub_ATR'].shift(1) <= df['close'].shift(1)
                )
            ), ['SMA10_sub_ATR_xup_close']
        ] = 1

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