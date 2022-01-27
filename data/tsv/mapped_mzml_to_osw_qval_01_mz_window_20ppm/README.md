## data/tsv/mapped_mzml_to_osw_qval_01_mz_window_20ppm

This directory is intended to store TSV files with the following properties:

- MZML data from `data/mzml/Rost_DIApy3_SP2um_90min_250ngK562_100nL_1_Slot1-5_1_1330_6-28-2021_0_2400_to_2700_swath_700.mzML`
- OSW features with QVALUE < 0.01 from `data/tsv/20220124_sig_qval_null_feature_ftrans_trans_score_ms2.tsv`
- MZ peak (from MZML file) cutoff range of +/- 20ppm

The equation for converting MZ to PPM is as follows

```python
def mz_to_ppm(mz, ppm):
    err_da = mz / (1_000_000 / ppm)
    return (mz - err_da, mz + err_da)
```

Total number of TSV files, as a reflection of the total number of retention times:

- 65 / 168 :: restart search @ index 65 when iterating through `MsExperiment` instance of MZML data.
