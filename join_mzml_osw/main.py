# BEGIN MERGING OF MZML FILE WITH OSW FILE
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyopenms as pms

ROOT_DIR = Path(__file__).parent.resolve().parent.absolute()
DATA_DIR = ROOT_DIR / "data"
MZML_DIR = DATA_DIR / "mzml"
OSW_DIR = DATA_DIR / "osw"
TSV_DIR = DATA_DIR / "tsv"
# For exporting temporary TSV files
TSV_TEMP_DIR = TSV_DIR / "temp"


def get_msexperiment_obj(mzml_filepath):
    # Read corresponding mzML file and load it into pyopenms object
    msexperiment = pms.MSExperiment()
    pms.MzMLFile().load(str(mzml_filepath), msexperiment)
    return msexperiment


def read_chunk_tsv(input_path, chunksize=100_000):
    # DON'T USE IF YOU ARE PLANNING TO MERGE TABLES ON A KEY
    print(f"Reading {input_path} as TSV chunk")
    with pd.read_csv(input_path, sep="\t", chunksize=chunksize) as reader:
        yield from reader


def fetch_product_mz(tsv_chunk, mz_range=0.001, ppm=None):
    for idx, chunk in enumerate(tsv_chunk):
        if ppm is not None and isinstance(ppm, (int, float)):
            ppm_range = (mz_to_ppm(mz, ppm) for mz in chunk["PRODUCT_MZ"])
            chunk["PRODUCT_MZ_LEFT"], chunk["PRODUCT_MZ_RIGHT"] = zip(*ppm_range)
        else:
            # Give a small width to the PRODUCT_MZ vals - Do 0.001 Da for now
            chunk["PRODUCT_MZ_LEFT"] = chunk["PRODUCT_MZ"] - mz_range
            chunk["PRODUCT_MZ_RIGHT"] = chunk["PRODUCT_MZ"] + mz_range
        print(f"Reading chunk #{idx}", end="\r")
        yield chunk


def mz_to_ppm(mz, ppm):
    err_da = mz / (1_000_000 / ppm)
    return (mz - err_da, mz + err_da)


def find_peak_in_mzml_to_osw(mzml_exp, osw_df):
    # Iterate through MZML experiments to find significant peaks
    for exp_idx, exp in enumerate(mzml_exp):
        output_tsv_filename = generate_output_mzml_tsv_filename(exp)
        output_mzml_df = generate_output_mzml_file_df_format()
        non_found = 0
        found = 0
        # MZ and intensity arrays
        mz_array, int_array = exp.get_peaks()
        # Iterate through MZ values in MZML experiment
        for mz_idx, mz in enumerate(mz_array):
            if mz_idx % 500 == 0 and mz_idx != 0:
                # Write to file every 500th loop - prevents bloating accumulating DF in memory
                output_tsv_filepath = TSV_TEMP_DIR / f"{mz_idx}_{output_tsv_filename}"
                export_as_tsv(output_mzml_df, output_tsv_filepath)
                output_mzml_df = generate_output_mzml_file_df_format()

            # Find MZ that lie within a MZ threshold as defined in OSW file
            filtered_osw_df = osw_df[
                (mz > osw_df["PRODUCT_MZ_LEFT"]) & (mz < osw_df["PRODUCT_MZ_RIGHT"])
            ]
            if filtered_osw_df.shape[0] != 0:
                # Peak(s) found - record data to output file
                found += 1
                output_mzml_df = pd.concat(
                    [
                        output_mzml_df,
                        concat_desired_features_to_df(
                            exp, mz_idx, mz_array, int_array, filtered_osw_df
                        ),
                    ]
                )
            else:
                non_found += 1

            print(f"Found: {found} | Not Found: {non_found}", end="\r")

        # Get percentage found peaks per MZML experiment
        perc_sig_peaks = round(found / (non_found + found) * 100, 2)
        # Add logic here to merge df and add perc_sig_peaks col
        export_mzml_df = read_tsv_in_dir_and_merge(TSV_TEMP_DIR)
        export_mzml_df["PERC_SIG_PEAKS"] = perc_sig_peaks

        # Sort dataframe by 'FEATURE_ID' -- THIS IS CRUCIAL FOR FOLLOWING STEP.
        export_mzml_df = export_mzml_df.sort_values(by=["FEATURE_ID"])
        print(f"Found: {found} | Not Found: {non_found}")
        print(f"% of sig peaks identified for exp {exp_idx + 1}:", perc_sig_peaks)
        # Export df
        export_tsv_filepath = TSV_DIR / f"merged_{output_tsv_filename}"
        export_as_tsv(export_mzml_df, export_tsv_filepath)
        print(f"{export_tsv_filepath} written to file.")


def read_tsv_in_dir_and_merge(tsv_dir):
    # tsv_dir is a Path obj
    tsv_files = tsv_dir.rglob("*.tsv")
    df = pd.DataFrame()
    for tsv in tsv_files:
        df = pd.concat([df, pd.read_csv(tsv, sep="\t")])
        # Delete file
        tsv.unlink()
    return df


def export_as_tsv(df, output_path):
    print("Exporting dataframe as TSV...")
    df.to_csv(output_path, sep="\t", index=False)


def generate_output_mzml_tsv_filename(exp):
    exp_id = exp.getNativeID()
    return f"{datetime.now().strftime('%Y%m%d')}_{exp_id}_qvalue_01.tsv"


def generate_output_mzml_file_df_format():
    colnames = [
        # From OSW file
        "FEATURE_ID",
        "TRANSITION_ID",
        # From MZML file
        "MZ",
        "INTENSITY",
        "TIC",
        "IM",
        "MS_LEVEL",
        "RT",
        "UNCHARGED_MASS",
    ]

    return pd.DataFrame(columns=colnames)


def concat_desired_features_to_df(exp, mz_idx, mz_arr, int_arr, filtered_osw_df):
    output_df = generate_output_mzml_file_df_format()
    # From OSW file
    output_df["FEATURE_ID"] = filtered_osw_df["FEATURE_ID"]
    output_df["TRANSITION_ID"] = filtered_osw_df["TRANSITION_ID"]
    # From MZML file
    output_df["TIC"] = exp.calculateTIC()
    # mz_idx is needed to index correct IM value from MZML experiment
    output_df["MZ"] = mz_arr[mz_idx]
    output_df["INTENSITY"] = int_arr[mz_idx]
    output_df["IM"] = exp.getFloatDataArrays()[0].get_data()[mz_idx]
    output_df["MS_LEVEL"] = exp.getMSLevel()
    output_df["RT"] = exp.getRT()
    output_df["UNCHARGED_MASS"] = exp.getPrecursors()[0].getUnchargedMass()
    return output_df


if __name__ == "__main__":
    mzml_filepath = (
        MZML_DIR
        / "Rost_DIApy3_SP2um_90min_250ngK562_100nL_1_Slot1-5_1_1330_6-28-2021_0_2400_to_2700_swath_700.mzML"
    )
    swath_exp = get_msexperiment_obj(mzml_filepath)

    sig_qval_joined_osw = (
        TSV_DIR / "20220127_run_1330_0_sig_qval_null_feature_ftrans_trans_score_ms2.tsv"
    )
    # Fetch peaks that fit within m/z window of +/- 0.0001
    # osw_df = pd.concat(fetch_product_mz(read_chunk_tsv(sig_qval_joined_osw), mz_range=0.0001))
    # Fetch peaks that fit within m/z window of +/- 20ppm
    osw_df = pd.concat(fetch_product_mz(read_chunk_tsv(sig_qval_joined_osw), ppm=20))

    find_peak_in_mzml_to_osw(swath_exp, osw_df)
