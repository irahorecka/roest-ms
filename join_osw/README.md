## join_osw/

This directory is intended to generate a joined TSV file from an OpenSWATH (OSW) file.

OSW file used:

- `merged_-1.osw`

Tables joined:

- `SCORE_MS2` with `FEATURE_TRANSITIONS` by col `FEATURE_ID` --> `SCORE_MS2_FEAT_TRANS`
- `SCORE_MS2_FEAT_TRANS` with `FEATURE` by col `FEATURE_ID` --> `SCORE_MS2_FEAT2_TRANS`
- `SCORE_MS2_FEAT2_TRANS` with `TRANSITION` by col `TRANSITION_ID` --> `SCORE_MS2_FEAT2_TRANS2`
- Filter `SCORE_MS2_FEAT2_TRANS2` by col `QVALUE` where `QVALUE` is less than 0.01 --> `SIG_SCORE_MS2_FEAT2_TRANS2`

Running python script:

```bash
$ python main.py
```
