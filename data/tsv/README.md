## data/tsv/

This directory is intended to store all TSV files for my fourth rotation with Hannes Rost.

## TSV's of interest:

- `20220124_qval_null_feature_ftrans_trans_score_ms2.tsv` -- Captures all features in `.osw` file with the following joins:

    - Join table `SCORE_MS2` with table `FEATURE_TRANSITIONS` by column `FEATURE_ID` --> `T1`
    - Join `T1` with table `FEATURES` by column `FEATURE_ID` --> `T2`
    - Join `T2` with table `TRANSITIONS` by column `TRANSITION_ID` --> `T3`
    - Export `T3` as `20220124_qval_null_feature_ftrans_trans_score_ms2.tsv`
    - Filter `T3` by column `QVALUE` where `QVALUE` < 0.01 --> `SigT3`
    - Export `SigT3` as `20220124_sig_qval_null_feature_ftrans_trans_score_ms2.tsv`

### Backup

2022-01-27 -- backup of entire `data/` to external harddrive. Unused content will be removed from local `data/`. Source `/Volumes/Ira's External Harddrive/Backups.backupdb/Ira’s MacBook Pro/2022-01-27-104234/Macintosh HD - Data/Users/irahorecka/Desktop/Harddrive_Desktop/PhD/University of Toronto/Rotations/Rost Lab/roest-ms/data` to fetch files if necessary.
