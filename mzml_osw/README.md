## mzml_osw/

This directory is intended to find and export significant peaks (MZ) found in an mzml file in comparison to
significant peaks (FEATURE (MZ) with QVALUE less than 0.01) found in an osw file.

OSW TSV file used:

- `data/tsv/20220124_sig_qval_null_feature_ftrans_trans_score_ms2.tsv` - sourced from `join_osw/main.py`

MZML file used:

- `data/mzml/Rost_DIApy3_SP2um_90min_250ngK562_100nL_1_Slot1-5_1_1330_6-28-2021_0_2400_to_2700_swath_700.mzML`

Exported file destination:

`data/tsv/mapped_mzml_to_osw_qval_01/`

Running python script:

```bash
$ python main.py
```