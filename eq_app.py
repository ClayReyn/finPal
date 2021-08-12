import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

''''''
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
    'SMA10_sub_ATR_xup_close'
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

''''''

def thirds_solox(wTi_df, quant_cols, pkid_prefix=''):
    ''' v_0.1 '''
    solo_rows = []
    pkid_int = 0
    for techcol in quant_cols:

        print(f'{techcol} @ {datetime.strftime(datetime.now(), "%H:%M:%S")}')
        
        pkid_int += 1
        testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}'

        tech_q000 = wTi_df[techcol].quantile(0.00)
        tech_q025 = wTi_df[techcol].quantile(0.25)
        tech_q050 = wTi_df[techcol].quantile(0.5)
        tech_q075 = wTi_df[techcol].quantile(0.75)
        tech_q100 = wTi_df[techcol].quantile(1.0)

        # under median
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q000, 
                right=tech_q050
            )
        ]
        obsv = len(subset_df)
        Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        solo_rows.append( 
            [ 
                f'{testname_pkid}u',
                techcol, 
                'q000_q050', 
                tech_q000, 
                tech_q050,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # interquartile range
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q025, 
                right=tech_q075
            )
        ]
        obsv = len(subset_df)
        Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        solo_rows.append( 
            [ 
                f'{testname_pkid}i',
                techcol, 
                'q025_q075', 
                tech_q025, 
                tech_q075,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # over median
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q050, 
                right=tech_q100
            )
        ]
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        solo_rows.append( 
            [ 
                f'{testname_pkid}o',
                techcol, 
                'q050_q100', 
                tech_q050, 
                tech_q100,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )

    thirds_solox_df = pd.DataFrame(
        data=solo_rows, 
        columns=[
                'pkid',
                'techcol', 
                'techzone', 
                'techleft',
                'techright', 
                'obsv', 
                'Ov0', 
                'Ov0c', 
                'Ov05', 
                'Ov05c', 
                'Ov1', 
                'Ov1c'
        ]
    )
    return thirds_solox_df


def thirds_2x(wTi_df, quant_cols, pkid_prefix=''):
    ''' v_0.1 '''
    thirds_rows = []
    pkid_int = 0
    for techcol in quant_cols:

        print(f'{techcol} @ {datetime.strftime(datetime.now(), "%H:%M:%S")}')

        pkid_int += 1
        testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}'
        
        tech_q000 = wTi_df[techcol].quantile(0.00)
        tech_q025 = wTi_df[techcol].quantile(0.25)
        tech_q050 = wTi_df[techcol].quantile(0.5)
        tech_q075 = wTi_df[techcol].quantile(0.75)
        tech_q100 = wTi_df[techcol].quantile(1.0)

        # under median
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q000, 
                right=tech_q050
            )
        ]
        pkid_int2 = 0
        for techcol2 in quant_cols:
            # if techcol2 == techcol:
            #     continue
            # else:
            #     pass
            pkid_int2 += 1
            testname_pkid2 = f'{techcol2[:2]}{pkid_int2}'

            tech2_q000 = wTi_df[techcol2].quantile(0.00)
            tech2_q050 = wTi_df[techcol2].quantile(0.50)
            dubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q000,
                right=tech2_q050,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}u_{testname_pkid2}u',
                    techcol, 
                    'q000_q050', 
                    tech_q000, 
                    tech_q050, 
                    techcol2, 
                    'q000_q050', 
                    tech2_q000, 
                    tech2_q050, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q025 = wTi_df[techcol2].quantile(0.25)
            tech2_q075 = wTi_df[techcol2].quantile(0.75)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q025,
                right=tech2_q075,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}u_{testname_pkid2}i',
                    techcol, 
                    'q000_q050', 
                    tech_q000, 
                    tech_q050, 
                    techcol2, 
                    'q025_q075', 
                    tech2_q025, 
                    tech2_q075, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q100 = wTi_df[techcol2].quantile(1.00)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q050,
                right=tech2_q100,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}u_{testname_pkid2}o',
                    techcol, 
                    'q000_q050', 
                    tech_q000, 
                    tech_q050, 
                    techcol2, 
                    'q050_q100', 
                    tech2_q050, 
                    tech2_q100, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )

        # interquartile range
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q025, 
                right=tech_q075
            )
        ]
        pkid_int2 = 0
        for techcol2 in quant_cols:
            # if techcol2 == techcol:
            #     continue
            # else:
            #     pass
            pkid_int2 += 1
            testname_pkid2 = f'{techcol2[:2]}{pkid_int2}'
            tech2_q000 = wTi_df[techcol2].quantile(0.00)
            tech2_q050 = wTi_df[techcol2].quantile(0.50)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q000,
                right=tech2_q050,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}i_{testname_pkid2}u',
                    techcol, 
                    'q025_q075', 
                    tech_q025, 
                    tech_q075, 
                    techcol2, 
                    'q000_q050', 
                    tech2_q000, 
                    tech2_q050, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q025 = wTi_df[techcol2].quantile(0.25)
            tech2_q075 = wTi_df[techcol2].quantile(0.75)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q025,
                right=tech2_q075,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}i_{testname_pkid2}i',
                    techcol, 
                    'q025_q075', 
                    tech_q025, 
                    tech_q075, 
                    techcol2, 
                    'q025_q075', 
                    tech2_q025, 
                    tech2_q075, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q100 = wTi_df[techcol2].quantile(1.00)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q050,
                right=tech2_q100,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}i_{testname_pkid2}o',
                    techcol, 
                    'q025_q075', 
                    tech_q025, 
                    tech_q075, 
                    techcol2, 
                    'q050_q100', 
                    tech2_q050, 
                    tech2_q100, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )

        # over median
        subset_df = wTi_df.loc[
            wTi_df[techcol].between(
                left=tech_q050, 
                right=tech_q100
            )
        ]
        pkid_int2 = 0
        for techcol2 in quant_cols:
            # if techcol2 == techcol:
            #     continue
            # else:
            #     pass
            pkid_int2 += 1
            testname_pkid2 = f'{techcol2[:2]}{pkid_int2}'
            tech2_q000 = wTi_df[techcol2].quantile(0.00)
            tech2_q050 = wTi_df[techcol2].quantile(0.50)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q000,
                right=tech2_q050,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}o_{testname_pkid2}u',
                    techcol, 
                    'q050_q100', 
                    tech_q050, 
                    tech_q100, 
                    techcol2, 
                    'q000_q050', 
                    tech2_q000, 
                    tech2_q050, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q025 = wTi_df[techcol2].quantile(0.25)
            tech2_q075 = wTi_df[techcol2].quantile(0.75)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q025,
                right=tech2_q075,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}o_{testname_pkid2}i',
                    techcol, 
                    'q050_q100', 
                    tech_q050, 
                    tech_q100, 
                    techcol2, 
                    'q025_q075', 
                    tech2_q025, 
                    tech2_q075, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            tech2_q100 = wTi_df[techcol2].quantile(1.00)
            dubsubset_df = subset_df.loc[
                subset_df[techcol2].between( 
                left=tech2_q050,
                right=tech2_q100,
                )
            ]
            obsv = len(dubsubset_df)
            Ov0 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(dubsubset_df.loc[dubsubset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            thirds_rows.append( 
                [ 
                    f'{testname_pkid}o_{testname_pkid2}o',
                    techcol, 
                    'q050_q100', 
                    tech_q050, 
                    tech_q100, 
                    techcol2, 
                    'q050_q100', 
                    tech2_q050, 
                    tech2_q100, 
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
        quant_cols.remove(techcol)

    thirds_2x_df = pd.DataFrame(
            data=thirds_rows, 
            columns=[ 
                'pkid',
                'techcol',
                'techzone',
                'techleft',
                'techright',
                'tech2',
                'tech2zone',
                'tech2left',
                'tech2right',
                'obsv',
                'Ov0',
                'Ov0c', 
                'Ov05', 
                'Ov05c', 
                'Ov1', 
                'Ov1c'
            ]
    )
    return thirds_2x_df


def prefix_p2m2(
    wTi_df, 
    quant_cols,
    p1tech, 
    p1left, 
    p1right, 
    p2tech, 
    p2left, 
    p2right
    ):
    ''' call a prefix, then route df to main combinator '''
    pkid_prefix = ( 
        f'{p1tech[:2]}{str(p1left)[:2]}{str(p1right)[:2]}_{p2tech[:2]}{str(p2left)[:2]}{str(p2right)[:2]}_'
    )
    p2_df = wTi_df.loc[ 
        ( 
            ( 
                wTi_df[p1tech].between( 
                    left=p1left,
                    right=p1right
                )
            ) & ( 
                wTi_df[p2tech].between( 
                    left=p2left,
                    right=p2right
                )
            )
        )
    ]
    thirds_4x_df = thirds_2x( 
        wTi_df=p2_df,
        quant_cols=quant_cols,
        pkid_prefix=pkid_prefix
    )
    thirds_4x_df['p1tech'] = p1tech
    thirds_4x_df['p1left'] = p1left
    thirds_4x_df['p1right'] = p1right
    thirds_4x_df['p2tech'] = p2tech
    thirds_4x_df['p2left'] = p2left
    thirds_4x_df['p2right'] = p2right
    return thirds_4x_df


def daybyday_solox(wTi_df, tests_df, test_pkid, stISOf, endISOf):
    tests_row = tests_df.loc[tests_df['pkid'] == test_pkid]
    if len(tests_row) < 1:
        print('pkid lookup failed')
        return None
    elif len(tests_row) > 1:
        print('pkid not unique')
        return None
    else:
        pass
    fromDate = date.fromisoformat(stISOf)
    toDate = date.fromisoformat(endISOf)
    date_list = [ datetime.strftime(toDate, '%Y-%m-%d') ]
    aDay = timedelta(days=1)
    prevDate = toDate - aDay
    while prevDate > fromDate:
        date_list.append(datetime.strftime(prevDate, '%Y-%m-%d'))
        prevDate = prevDate - aDay

    dbd_rows = []
    for aDate in date_list:
        aDate_df = wTi_df.loc[ 
            ( 
                ( 
                    wTi_df[tests_row.at[0, 'techcol']].between( 
                        left=tests_row.at[0, 'techleft'],
                        right=tests_row.at[0, 'techright']
                    )
                ) & ( 
                    wTi_df['date'] == aDate
                )
            )
        ]
        obsv = len(aDate_df)
        Ov0 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0
            ]
        )
        Ov05 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0.5
            ]
        )
        Ov1 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 1
            ]
        )
        if obsv > 0:
            Ov0c = Ov0 / obsv
            Ov05c = Ov05 / obsv
            Ov1c = Ov1 / obsv
            obsv_tickers = aDate_df['ticker'].tolist()
            Ov0_tickers = aDate_df.loc[
                aDate_df['close_chngpct_24_tmro'] > 0
            ]['ticker'].tolist()
        else:
            Ov0c = np.NaN
            Ov05c = np.NaN
            Ov1c = np.NaN
            obsv_tickers = []
            Ov0_tickers = []
        dbd_rows.append( 
            [ 
                test_pkid,
                aDate,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c,
                obsv_tickers,
                Ov0_tickers
            ]
        )
    dbd_df = pd.DataFrame( 
        data= dbd_rows,
        columns= [ 
            'test_pkid',
            'date',
            'obsv',
            'Ov0',
            'Ov0c',
            'Ov05',
            'Ov05c',
            'Ov1',
            'Ov1c',
            'obsv_tickers',
            'Ov0_tickers'
        ]
    )
    return dbd_df


def daybyday_2x(wTi_df, tests_df, test_pkid, stISOf, endISOf):
    tests_row = tests_df.loc[tests_df['pkid'] == test_pkid]
    if len(tests_row) < 1:
        print('pkid lookup failed')
        return None
    elif len(tests_row) > 1:
        print('pkid not unique')
        return None
    else:
        pass
    fromDate = date.fromisoformat(stISOf)
    toDate = date.fromisoformat(endISOf)
    date_list = [ datetime.strftime(toDate, '%Y-%m-%d') ]
    aDay = timedelta(days=1)
    prevDate = toDate - aDay
    while prevDate > fromDate:
        date_list.append(datetime.strftime(prevDate, '%Y-%m-%d'))
        prevDate = prevDate - aDay
    
    dbd_rows = []
    for aDate in date_list:
        aDate_df = wTi_df.loc[ 
            ( 
                ( 
                    wTi_df[tests_row.at[0, 'techcol']].between( 
                        left=tests_row.at[0, 'techleft'],
                        right=tests_row.at[0, 'techright']
                    )
                ) & ( 
                    wTi_df[tests_row.at[0, 'tech2']].between( 
                        left=tests_row.at[0, 'tech2left'],
                        right=tests_row.at[0, 'tech2right']
                    )
                ) & ( 
                    wTi_df['date'] == aDate
                )
            )
        ]
        obsv = len(aDate_df)
        Ov0 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0
            ]
        )
        Ov05 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0.5
            ]
        )
        Ov1 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 1
            ]
        )
        if obsv > 0:
            Ov0c = Ov0 / obsv
            Ov05c = Ov05 / obsv
            Ov1c = Ov1 / obsv
            obsv_tickers = aDate_df['ticker'].tolist()
            Ov0_tickers = aDate_df.loc[
                aDate_df['close_chngpct_24_tmro'] > 0
            ]['ticker'].tolist()
        else:
            Ov0c = np.NaN
            Ov05c = np.NaN
            Ov1c = np.NaN
            obsv_tickers = []
            Ov0_tickers = []
        dbd_rows.append( 
            [ 
                test_pkid,
                aDate,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c,
                obsv_tickers,
                Ov0_tickers
            ]
        )
    dbd_df = pd.DataFrame( 
        data= dbd_rows,
        columns= [ 
            'test_pkid',
            'date',
            'obsv',
            'Ov0',
            'Ov0c',
            'Ov05',
            'Ov05c',
            'Ov1',
            'Ov1c',
            'obsv_tickers',
            'Ov0_tickers'
        ]
    )
    return dbd_df


def daybyday_4x(wTi_df, tests_df, test_pkid, stISOf, endISOf):
    tests_row = tests_df.loc[tests_df['pkid'] == test_pkid]
    if len(tests_row) < 1:
        print('pkid lookup failed')
        return None
    elif len(tests_row) > 1:
        print('pkid not unique')
        return None
    else:
        pass
    fromDate = date.fromisoformat(stISOf)
    toDate = date.fromisoformat(endISOf)
    date_list = [ datetime.strftime(toDate, '%Y-%m-%d') ]
    aDay = timedelta(days=1)
    prevDate = toDate - aDay
    while prevDate > fromDate:
        date_list.append(datetime.strftime(prevDate, '%Y-%m-%d'))
        prevDate = prevDate - aDay
    
    dbd_rows = []
    for aDate in date_list:
        aDate_df = wTi_df.loc[ 
            ( 
                ( 
                    wTi_df[tests_row.at[0, 'techcol']].between( 
                        left=tests_row.at[0, 'techleft'],
                        right=tests_row.at[0, 'techright']
                    )
                ) & ( 
                    wTi_df[tests_row.at[0, 'tech2']].between( 
                        left=tests_row.at[0, 'tech2left'],
                        right=tests_row.at[0, 'tech2right']
                    )
                ) & ( 
                    wTi_df[tests_row.at[0, 'p1tech']].between( 
                        left=tests_row.at[0, 'p1left'],
                        right=tests_row.at[0, 'p1right']
                    )
                ) & ( 
                    wTi_df[tests_row.at[0, 'p2tech']].between( 
                        left=tests_row.at[0, 'p2left'],
                        right=tests_row.at[0, 'p2right']
                    )
                ) & ( 
                    wTi_df['date'] == aDate
                )
            )
        ]
        obsv = len(aDate_df)
        Ov0 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0
            ]
        )
        Ov05 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 0.5
            ]
        )
        Ov1 = len( 
            aDate_df.loc[ 
                aDate_df['close_chngpct_24_tmro'] > 1
            ]
        )
        if obsv > 0:
            Ov0c = Ov0 / obsv
            Ov05c = Ov05 / obsv
            Ov1c = Ov1 / obsv
            obsv_tickers = aDate_df['ticker'].tolist()
            Ov0_tickers = aDate_df.loc[
                aDate_df['close_chngpct_24_tmro'] > 0
            ]['ticker'].tolist()
        else:
            Ov0c = np.NaN
            Ov05c = np.NaN
            Ov1c = np.NaN
            obsv_tickers = []
            Ov0_tickers = []
        dbd_rows.append( 
            [ 
                test_pkid,
                aDate,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c,
                obsv_tickers,
                Ov0_tickers
            ]
        )
    dbd_df = pd.DataFrame( 
        data= dbd_rows,
        columns= [ 
            'test_pkid',
            'date',
            'obsv',
            'Ov0',
            'Ov0c',
            'Ov05',
            'Ov05c',
            'Ov1',
            'Ov1c',
            'obsv_tickers',
            'Ov0_tickers'
        ]
    )
    return dbd_df


###

def testfilter_solox(
    wTi_df, 
    test_cols=_pm_stack_test_list, 
    quant_set=_pm_stack_quant_set, 
    binary_set=_pm_stack_binary_set,
    radio_set=_pm_stack_radio_set,
    pkid_prefix=''
    ):
    ''' v_0.2 with binary conditions '''
    solo_rows = []
    pkid_int = 0
    for techcol in test_cols:
        print(f'{techcol} @ {datetime.strftime(datetime.now(), "%H:%M:%S")}')
        pkid_int += 1
        testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}'

        if techcol in binary_set:
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=1, 
                    right=1
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}y',
                    techcol, 
                    'yes', 
                    1, 
                    1,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
        elif techcol in radio_set:
            # 0 - 20
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=0, 
                    right=25
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}r25',
                    techcol, 
                    'r25', 
                    0, 
                    25,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # 0 - 30
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=0, 
                    right=35
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}r35',
                    techcol, 
                    'r35', 
                    0, 
                    35,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # 30 - 70
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=35, 
                    right=65
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}rm',
                    techcol, 
                    'rm', 
                    35, 
                    65,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # 70 - 100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=65, 
                    right=100
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}r65',
                    techcol, 
                    'r65', 
                    65, 
                    100,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # 80 - 100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=75, 
                    right=100
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}r75',
                    techcol, 
                    'r75', 
                    75, 
                    100,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # as if quant
            tech_q000 = wTi_df[techcol].quantile(0.00)
            tech_q025 = wTi_df[techcol].quantile(0.25)
            tech_q050 = wTi_df[techcol].quantile(0.5)
            tech_q075 = wTi_df[techcol].quantile(0.75)
            tech_q100 = wTi_df[techcol].quantile(1.0)
            # under median
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q000, 
                    right=tech_q050
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}u',
                    techcol, 
                    'q000_q050', 
                    tech_q000, 
                    tech_q050,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # interquartile range
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q025, 
                    right=tech_q075
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}i',
                    techcol, 
                    'q025_q075', 
                    tech_q025, 
                    tech_q075,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # over median
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q050, 
                    right=tech_q100
                )
            ]
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}o',
                    techcol, 
                    'q050_q100', 
                    tech_q050, 
                    tech_q100,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
        elif techcol in quant_set:
            tech_q000 = wTi_df[techcol].quantile(0.00)
            tech_q025 = wTi_df[techcol].quantile(0.25)
            tech_q050 = wTi_df[techcol].quantile(0.5)
            tech_q075 = wTi_df[techcol].quantile(0.75)
            tech_q100 = wTi_df[techcol].quantile(1.0)
            # under median
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q000, 
                    right=tech_q050
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}u',
                    techcol, 
                    'q000_q050', 
                    tech_q000, 
                    tech_q050,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # interquartile range
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q025, 
                    right=tech_q075
                )
            ]
            obsv = len(subset_df)
            Ov0 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0])
            Ov05 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 0.5])
            Ov1 = len(subset_df.loc[subset_df['close_chngpct_24_tmro'] > 1])
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}i',
                    techcol, 
                    'q025_q075', 
                    tech_q025, 
                    tech_q075,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
            # over median
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=tech_q050, 
                    right=tech_q100
                )
            ]
            if obsv > 0:
                Ov0c = (Ov0 / obsv)
                Ov05c = (Ov05 / obsv)
                Ov1c = (Ov1 / obsv)
            else:
                Ov0c = 0
                Ov05c = 0
                Ov1c = 0
            solo_rows.append( 
                [ 
                    f'{testname_pkid}o',
                    techcol, 
                    'q050_q100', 
                    tech_q050, 
                    tech_q100,
                    obsv,
                    Ov0,
                    Ov0c,
                    Ov05,
                    Ov05c,
                    Ov1,
                    Ov1c
                ]
            )
        else:
            print(f'missing classification: {techcol}')
            continue
    thirds_solox_df = pd.DataFrame(
        data=solo_rows, 
        columns=[
                'pkid',
                'techcol', 
                'techzone', 
                'techleft',
                'techright', 
                'obsv', 
                'Ov0', 
                'Ov0c', 
                'Ov05', 
                'Ov05c', 
                'Ov1', 
                'Ov1c'
        ]
    )
    return thirds_solox_df


def _crossfilter_lvl2(
    wTi_df,
    subset_df,
    testname_pkid,
    techcol,
    subzone,
    subleft,
    subright,
    techcol2, 
    pkid_int2,
    quant_set=_pm_stack_quant_set,
    binary_set=_pm_stack_binary_set,
    radio_set=_pm_stack_radio_set,
    ):
    ''' innerloop of crossfilter_2x '''
    lvl2_rows = []
    testname_pkid2 = f'{techcol2[:2]}{pkid_int2}'

    if techcol2 in binary_set:
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=1, 
                right=1
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}y',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'yes', 
                1, 
                1,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
    # radio set
    elif techcol2 in radio_set:
        # 0 - 20
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=0, 
                right=25
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}r25',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'r25', 
                0, 
                25,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # 0 - 30
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=0, 
                right=35
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}r35',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'r35', 
                0, 
                35,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # 35 - 70
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=35, 
                right=65
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}rm',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'rm', 
                35, 
                65,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # 65 - 100
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=65, 
                right=100
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}r65',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'r65', 
                65, 
                100,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # 75 - 100
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=75, 
                right=100
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}r75',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'r75', 
                75, 
                100,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # as if quant
        tech2_q000 = wTi_df[techcol2].quantile(0.00)
        tech2_q025 = wTi_df[techcol2].quantile(0.25)
        tech2_q050 = wTi_df[techcol2].quantile(0.5)
        tech2_q075 = wTi_df[techcol2].quantile(0.75)
        tech2_q100 = wTi_df[techcol2].quantile(1.0)
        # under median
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q000, 
                right=tech2_q050
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}u',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q000_q050', 
                tech2_q000, 
                tech2_q050,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # interquartile range
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q025, 
                right=tech2_q075
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}i',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q025_q050', 
                tech2_q025, 
                tech2_q075,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # over median
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q050, 
                right=tech2_q100
            )
        ]
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}o',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q050_q100', 
                tech2_q050, 
                tech2_q100,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
    elif techcol2 in quant_set:
        tech2_q000 = wTi_df[techcol2].quantile(0.00)
        tech2_q025 = wTi_df[techcol2].quantile(0.25)
        tech2_q050 = wTi_df[techcol2].quantile(0.5)
        tech2_q075 = wTi_df[techcol2].quantile(0.75)
        tech2_q100 = wTi_df[techcol2].quantile(1.0)
        # under median
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q000, 
                right=tech2_q050
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}u',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q000_q050', 
                tech2_q000, 
                tech2_q050,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # interquartile range
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q025, 
                right=tech2_q075
            )
        ]
        obsv = len(dubset_df)
        Ov0 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0])
        Ov05 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 0.5])
        Ov1 = len(dubset_df.loc[dubset_df['close_chngpct_24_tmro'] > 1])
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}i',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q025_q050', 
                tech2_q025, 
                tech2_q075,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
        # over median
        dubset_df = subset_df.loc[
            subset_df[techcol2].between(
                left=tech2_q050, 
                right=tech2_q100
            )
        ]
        if obsv > 0:
            Ov0c = (Ov0 / obsv)
            Ov05c = (Ov05 / obsv)
            Ov1c = (Ov1 / obsv)
        else:
            Ov0c = 0
            Ov05c = 0
            Ov1c = 0
        lvl2_rows.append( 
            [ 
                f'{testname_pkid}{testname_pkid2}o',
                techcol, 
                subzone,
                subleft,
                subright,
                techcol2,
                'q050_q100', 
                tech2_q050, 
                tech2_q100,
                obsv,
                Ov0,
                Ov0c,
                Ov05,
                Ov05c,
                Ov1,
                Ov1c
            ]
        )
    else:
        print(f'missing classification: {techcol2}')
    return lvl2_rows


def crossfilter_2x( 
    wTi_df,
    test_cols=_pm_stack_test_list, 
    quant_set=_pm_stack_quant_set, 
    binary_set=_pm_stack_binary_set,
    radio_set=_pm_stack_radio_set,
    pkid_prefix=''
    ):
    ''' v_0.2 with binary and radio'''
    dub_rows = []
    pkid_int = 0
    for techcol in test_cols:
        print(f'{techcol} @ {datetime.strftime(datetime.now(), "%H:%M:%S")}')
        pkid_int += 1

        # t_1 if: binary
        if techcol in binary_set:
            # t_1 binary: yes
            subzone = 'yes'
            subleft = 1
            subright = 1
            subset_df = wTi_df.loc[ 
                wTi_df[techcol].between( 
                    left=subleft,
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}y'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)

        # t_1 if: radio
        elif techcol in radio_set:
            # t_1 radio: 0 - 25
            subzone = 'r25'
            subleft = 0
            subright = 25
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}r25'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: 0 - 35
            subzone = 'r35'
            subleft = 0
            subright = 35
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}r35'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: 35 - 65
            subzone = 'rm'
            subleft = 35
            subright = 65
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}rm'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: 65 - 100
            subzone = 'r65'
            subleft = 65
            subright = 100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}r65'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: 75 - 100
            subzone = 'r75'
            subleft = 75
            subright = 100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}r75'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: as if quant
            tech_q000 = wTi_df[techcol].quantile(0.00)
            tech_q025 = wTi_df[techcol].quantile(0.25)
            tech_q050 = wTi_df[techcol].quantile(0.5)
            tech_q075 = wTi_df[techcol].quantile(0.75)
            tech_q100 = wTi_df[techcol].quantile(1.0)
            # t_1 radio: q000-q050
            subzone = 'q000_q050'
            subleft = tech_q000
            subright = tech_q050
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}u'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: q025-q075
            subzone = 'q025_q075'
            subleft = tech_q025
            subright = tech_q075
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}i'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 radio: q050-q100
            subzone = 'q050_q100'
            subleft = tech_q050
            subright = tech_q100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}o'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
        
        # t_1 if: quant
        elif techcol in quant_set:
            tech_q000 = wTi_df[techcol].quantile(0.00)
            tech_q025 = wTi_df[techcol].quantile(0.25)
            tech_q050 = wTi_df[techcol].quantile(0.5)
            tech_q075 = wTi_df[techcol].quantile(0.75)
            tech_q100 = wTi_df[techcol].quantile(1.0)
            # t_1 quant: q000-q050
            subzone = 'q000_q050'
            subleft = tech_q000
            subright = tech_q050
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}u'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 quant: q025-q075
            subzone = 'q025_q075'
            subleft = tech_q025
            subright = tech_q075
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}i'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
            # t_1 quant: q050-q100
            subzone = 'q050_q100'
            subleft = tech_q050
            subright = tech_q100
            subset_df = wTi_df.loc[
                wTi_df[techcol].between(
                    left=subleft, 
                    right=subright
                )
            ]
            testname_pkid = f'{pkid_prefix}{techcol[:2]}{pkid_int}o'
            # lvl 2
            pkid_int2 = 0
            for techcol2 in test_cols:
                pkid_int2 += 1
                lvl2_rows = _crossfilter_lvl2( 
                    wTi_df=wTi_df,
                    subset_df=subset_df,
                    testname_pkid=testname_pkid,
                    techcol=techcol,
                    subzone=subzone,
                    subleft=subleft,
                    subright=subright,
                    techcol2=techcol2,
                    pkid_int2=pkid_int2
                )
                for lvl2_row in lvl2_rows:
                    dub_rows.append(lvl2_row)
    
        else:
            print(f'missing classification: {techcol}')
            continue
        
        test_cols.remove(techcol)

    crossfilter_2x_df = pd.DataFrame(
        data=dub_rows,
        columns=[ 
            'testname',
            'tech1',
            'techzone',
            'techleft',
            'techright',
            'tech2',
            'tech2zone',
            'tech2left',
            'tech2right',
            'obsv',
            'Ov0',
            'Ov0c',
            'Ov05',
            'Ov05c',
            'Ov1',
            'Ov1c'
        ]
    )
    return crossfilter_2x_df
        
