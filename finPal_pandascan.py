import numpy as np
import pandas as pd


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


    def bop__close_24_sub_n_pctf_1wk_low(cls, df) -> None:
        ''' 
        0.817 | 1400/1715 | mo @ august 7 
        '''
        recent_Ov0c = 0.816
        positive_idx = df.loc[
            (
                (
                    df['bop'].between(
                        left = -0.035, right= 1
                    )
                )
                & (df['close_24_sub_n_pctf_1wk_low'].between(
                    left = -3.635, right= 0.990
                    )
                )
            )
        ].index
        cls.blip_array.append(
            [ recent_Ov0c, 'bop__close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
        )

    
    def bop__normBP_chngpct_24__close_24_sub_n_pctf_1wk_low(cls, df) -> None:
            ''' 
            0.816 | 1400/1715 | mo @ august 7 
            '''
            recent_Ov0c = 0.816
            positive_idx = df.loc[
                (
                    (
                        df['bop'].between(
                            left = -0.035, right= 1
                        )
                    )
                    & (
                        df['normBP_chngpct_24'].between(
                            left = -3.822, right= 21982
                        )
                    )
                    & (df['close_24_sub_n_pctf_1wk_low'].between(
                        left = -3.635, right= 0.990
                        )
                    )
                )
            ].index
            cls.blip_array.append(
                [ recent_Ov0c, 'bop_volume_vwap_close_24_sub_n_pctf_1wk_low', len(positive_idx), positive_idx]
            )

    def idk():
        pass

    


    def return_blip_df(cls):
        ''' access blips in ohlcv via df.loc[blip_df.at(i, 'blips_idx')] '''
        blip_df = pd.DataFrame(data=cls.blip_array, columns=['recent_Ov0c', 'filters', 'blips', 'blips_idx'])
        return blip_df


    def Ov0c_of(cls, df, Ov0_cutoff, test_idx, test_name) -> list:
        ''' return can be nested and framed '''
        obsv = len(df.loc[test_idx])
        Ov0 = len(df.loc[test_idx].loc[df['close_chngpct_24_tmro'] > Ov0_cutoff])
        Ov0c = Ov0 / obsv
        return [test_name, Ov0_cutoff, obsv, Ov0, Ov0c]