from pathlib import Path

import pandas as pd

from query_osw import get_osw_table_as_df
import pd_util as pu

ROOT_DIR = Path(__file__).parent.resolve().parent.absolute()
DATA_DIR = ROOT_DIR / "data"
MZML_DIR = DATA_DIR / "mzml"
OSW_DIR = DATA_DIR / "osw"
TSV_DIR = DATA_DIR / "tsv"


def get_sig_score_ms2(osw_filepath, qvalue=0.01):
    score_ms2 = get_osw_table_as_df(osw_filepath, tablename="SCORE_MS2", chunksize=100_000)

    def filter_sig_score(chunk):
        return chunk[chunk["QVALUE"] < qvalue]

    return pd.concat(pu.chunked_apply(score_ms2, func=filter_sig_score))


def merge_feature_transitions_w_df(osw_filepath, df):
    feature_transitions = get_osw_table_as_df(osw_filepath, tablename="FEATURE_TRANSITION", chunksize=100_000)

    def merge_transitions_and_df(chunk):
        chunk = pu.exclude_dtypes(chunk, "object")
        chunk = pu.drop_columns(chunk, [col for col in list(chunk) if col.startswith("VAR")])
        # Both df must have col 'FEATURE_ID'
        return pu.merge_on_column(df, chunk, on="FEATURE_ID")

    return pd.concat(pu.chunked_apply(feature_transitions, func=merge_transitions_and_df))


def merge_feature_w_df(osw_filepath, df):
    features = get_osw_table_as_df(osw_filepath, tablename="FEATURE", chunksize=100_000)

    def merge_feature_and_df(chunk):
        chunk = pu.exclude_dtypes(chunk, "object")
        chunk = pu.rename_column(chunk, from_colname="ID", to_colname="FEATURE_ID")
        # Both df must have col 'FEATURE_ID'
        return pu.merge_on_column(df, chunk, on="FEATURE_ID")

    return pd.concat(pu.chunked_apply(features, func=merge_feature_and_df))


def get_sig_score(df, qvalue=0.01):
    def filter_sig_score(chunk):
        try:
            return chunk[chunk["QVALUE"] < qvalue]
        except TypeError:
            chunk = pu.set_col_dtype(chunk, "QVALUE", "float64")
            return chunk[chunk["QVALUE"] < qvalue]

    return pd.concat(pu.chunked_apply(df, func=filter_sig_score))


def merge_tras_w_df(osw_filepath, df):
    trans = get_osw_table_as_df(osw_filepath, tablename="TRANSITION", chunksize=100_000)

    def merge_trans_and_df(chunk):
        chunk = pu.exclude_dtypes(chunk, "object")
        chunk = pu.rename_column(chunk, from_colname="ID", to_colname="TRANSITION_ID")
        # Both df must have col 'TRANSITION_ID'
        return pu.merge_on_column(df, chunk, on="TRANSITION_ID")

    return pd.concat(pu.chunked_apply(trans, func=merge_trans_and_df))


if __name__ == "__main__":
    osw_filepath = OSW_DIR / "merged_-1.osw"
    ## [OPTIONAL]
    ## EXPORT SIGNIFICANT SCORE_MS2 SPECTRUM WITH QVALUE < 0.01
    # sig_score_ms2 = get_sig_score_ms2(osw_filepath, qvalue=0.01)
    # sig_score_ms2_filepath = TSV_DIR / "20220124_qval_01_score_ms2.tsv"
    # pu.export_as_tsv(sig_score_ms2, sig_score_ms2_filepath)

    # ========== START PIPELINE ==========

    ## EXPORT SCORE_MS2 SPECTRUM WITH QVALUE < 1 - I.E., ALL ROWS
    # score_ms2 = get_sig_score_ms2(osw_filepath, qvalue=1)
    # score_ms2_filepath = TSV_DIR / "20220124_qval_null_score_ms2.tsv"
    # pu.export_as_tsv(score_ms2, score_ms2_filepath)

    ## MERGE SCORE_MS2 WITH QVALUE < 1 DF WITH FEATURE_TRANSITIONS ON COL 'FEATURE_ID'
    # score_ms2 = pu.read_tsv(score_ms2_filepath)
    # ftrans_score_ms2 = merge_feature_transitions_w_df(osw_filepath, score_ms2)
    # ftrans_score_ms2_filepath = TSV_DIR / "20220124_qval_null_ftrans_score_ms2.tsv"
    # pu.export_as_tsv(ftrans_score_ms2, ftrans_score_ms2_filepath)

    ## MERGE FTRANS_SCORE_MS2 WITH QVALUE < 1 DF WITH FEATURES ON COL 'FEATURE_ID'
    # ftrans_score_ms2 = pu.read_tsv(ftrans_score_ms2_filepath)
    # ftrans_feature_score_ms2 = merge_feature_w_df(osw_filepath, ftrans_score_ms2)
    # ftrans_feature_score_ms2_filepath = TSV_DIR / "20220124_qval_null_feature_ftrans_score_ms2.tsv"
    # pu.export_as_tsv(ftrans_feature_score_ms2, ftrans_feature_score_ms2_filepath)

    ## MERGE FTRANS_FEATURE_SCORE_MS2 WITH QVALUE < 1 DF WITH TRANSITIONS ON COL 'TRANSITION_ID'
    # ftrans_feature_score_ms2 = pu.read_tsv(ftrans_feature_score_ms2_filepath)
    # ftrans_feature_trans_score_ms2 = merge_tras_w_df(osw_filepath, ftrans_feature_score_ms2)
    ftrans_feature_trans_score_ms2_filepath = (
        TSV_DIR / "20220124_qval_null_feature_ftrans_trans_score_ms2.tsv"
    )
    # pu.export_as_tsv(ftrans_feature_trans_score_ms2, ftrans_feature_trans_score_ms2_filepath)

    ## EXPORT SIGNIFICANT FTRANS_FEATURE_TRANS_SCORE_MS2 SPECTRUM WITH QVALUE < 0.01
    ftrans_feature_trans_score_ms2 = pu.read_chunk_tsv(ftrans_feature_trans_score_ms2_filepath)
    sig_ftrans_feature_trans_score_ms2 = get_sig_score(ftrans_feature_trans_score_ms2, qvalue=0.01)
    sig_ftrans_feature_trans_score_ms2_filepath = (
        TSV_DIR / "20220124_sig_qval_null_feature_ftrans_trans_score_ms2.tsv"
    )
    pu.export_as_tsv(
        sig_ftrans_feature_trans_score_ms2, sig_ftrans_feature_trans_score_ms2_filepath
    )
