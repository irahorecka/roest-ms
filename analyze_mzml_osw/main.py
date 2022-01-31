# BEGIN MERGING OF MZML FILE WITH OSW FILE
from pathlib import Path

import pandas as pd

import pd_util as pu

ROOT_DIR = Path(__file__).parent.resolve().parent.absolute()
DATA_DIR = ROOT_DIR / "data"
MZML_DIR = DATA_DIR / "mzml"
OSW_DIR = DATA_DIR / "osw"
TSV_DIR = DATA_DIR / "tsv"


def tidy_merged_mzml_osw_df(merged_df):
    print("Tidying Merged MZML and OSW DataFrame")
    cols_to_rm = [
        "PVALUE",
        "PEP",
        "UNCHARGED_MASS",
        "DETECTING",
        "IDENTIFYING",
        "QUANTIFYING",
        "DECOY",
    ]
    merged_df_tidy = merged_df.drop(columns=cols_to_rm)
    # Drop duplicate rows
    return merged_df_tidy.drop_duplicates()


def aggregate_merged_mzml_osw_df(merged_df):
    print("Aggregating Tidied Merged MZML and OSW DataFrame")
    # Aggregate data using a pivot table
    # Aggregate FEATURE_ID and MZ, keeping unique features
    merged_df_pivot = pd.pivot_table(
        merged_df,
        index=["FEATURE_ID", "MZ"],
        aggfunc={
            "IM": lambda x: unique(x),
            "INTENSITY": lambda x: unique(x),
            "QVALUE": lambda x: unique(x),
            "NORM_RT": lambda x: unique(x),
            "EXP_RT": lambda x: unique(x),
            "EXP_IM": lambda x: unique(x),
            # You CAN use np.unique (probably more performant), but you run into aggregation issues
        },
    ).reset_index()
    # Aggregate ID (Feature ID) with emphasis on determining number of MZ, IM, and INTENSITY per Feature ID
    merged_df_pivot = pd.pivot_table(
        merged_df_pivot,
        index=["FEATURE_ID"],
        aggfunc={
            "IM": lambda x: list(x),
            "MZ": lambda x: list(x),
            "INTENSITY": lambda x: list(x),
            "QVALUE": lambda x: unique(x),
            "NORM_RT": lambda x: unique(x),
            "EXP_RT": lambda x: unique(x),
            "EXP_IM": lambda x: unique(x),
        },
    ).reset_index()

    return merged_df_pivot


def unique(arr):
    unique_arr = tuple(set(arr))
    if len(unique_arr) == 1:
        return unique_arr[0]
    return unique_arr


def summarize_agg_merged_mzml_osw_df(agg_merged_df):
    print("Summarizing Aggregated & Tidied Merged MZML and OSW DataFrame")

    def compress_list(l):
        concat = []
        for item in l:
            if hasattr(item, "__iter__"):
                concat.extend(item)
            else:
                concat.append(item)
        return concat

    agg_merged_df["IM_FLAT"] = agg_merged_df["IM"].apply(lambda x: sorted(compress_list(x)))
    agg_merged_df["IM_COUNT"] = agg_merged_df["IM"].apply(lambda x: len(compress_list(x)))
    agg_merged_df["MZ_COUNT"] = agg_merged_df["MZ"].apply(lambda x: len(compress_list(x)))
    agg_merged_df["INTENSITY_FLAT"] = agg_merged_df["INTENSITY"].apply(lambda x: compress_list(x))
    agg_merged_df["INTENSITY_COUNT"] = agg_merged_df["INTENSITY"].apply(
        lambda x: len(compress_list(x))
    )

    return agg_merged_df


def main(chunked_df, sig_qvalue_osw_df):
    for idx, chunk in enumerate(chunked_df):
        print(f"Processing chunk #{idx + 1}...")
        # Merge joined MZML/OSW file (df) with significant QVALUE OSW file (df) by column 'FEATURE_ID'
        merged_mzml_osw = pu.merge_on_column(
            df_left=chunk, df_right=sig_qvalue_osw_df, on="FEATURE_ID"
        )
        # Tidy merged df
        merged_mzml_osw_tidied = tidy_merged_mzml_osw_df(merged_mzml_osw)
        # Aggregate tidied merged df
        agg_merged_mzml_osw_tidied = aggregate_merged_mzml_osw_df(merged_mzml_osw_tidied)
        # Summarize aggregated tidied merged df
        summarized_agg_merged_mzml_osw_tidied = summarize_agg_merged_mzml_osw_df(
            agg_merged_mzml_osw_tidied
        )
        # Export file
        pu.export_as_tsv(
            summarized_agg_merged_mzml_osw_tidied, TSV_DIR / f"test_summ_chunk_#{idx + 1}.tsv"
        )
        # Maybe helps with memory?
        del (
            merged_mzml_osw,
            merged_mzml_osw_tidied,
            agg_merged_mzml_osw_tidied,
            summarized_agg_merged_mzml_osw_tidied,
        )


if __name__ == "__main__":
    # Joined MZML/OSW file (df)
    mzml_osw_qval_001_mz_window_001_dir = TSV_DIR / "mapped_mzml_to_osw_qval_01_mz_window_20ppm"
    # !!! IMPORTED DATAFRAME MUST BE SORTED BY 'FEATURE_ID' !!!
    print("Load joined MZML/OSW file...")
    mzml_osw_1_168_qval_001_mz_window_001 = pu.read_chunk_tsv(
        # mzml_osw_qval_001_mz_window_001_dir / "merged_20220126_frame=22786_scan=452_qvalue_01.tsv",
        TSV_DIR / "merged_20220127_frame=22786_scan=452_qvalue_01.tsv",
        chunksize=1_000_000,
    )
    # Significant QVALUE OSW file (df)
    print("Load significant QVALUE OSW file...")
    sig_qvalue_osw = pd.read_csv(
        TSV_DIR / "20220124_sig_qval_null_feature_ftrans_trans_score_ms2.tsv", sep="\t"
    )

    main(mzml_osw_1_168_qval_001_mz_window_001, sig_qvalue_osw)
