"""
For all things iterable regarding pandas df
"""
import pandas as pd


def chunked_apply(chunked_df, *args, func=None, **kwargs):
    for idx, chunk in enumerate(chunked_df):
        # apply func to chunk and yield
        print(f"Applying func {func.__name__} to chunk #{idx + 1}", end="\r")
        if func is None:
            yield chunk
        else:
            yield func(chunk, *args, **kwargs)
    print()


def merge_on_column(df_left, df_right, rm_obj_type=True, **kwargs):
    if rm_obj_type:
        df_left = exclude_dtypes(df_left, "object")
        df_right = exclude_dtypes(df_right, "object")
    return df_left.merge(df_right, **kwargs)


def exclude_dtypes(df, dtypes):
    if not isinstance(dtypes, (tuple, list)):
        return df.select_dtypes(exclude=[dtypes])
    return df.select_dtypes(exclude=dtypes)


def set_col_dtype(df, colname, dtype):
    return df.astype({colname: dtype})


def get_df_columns(df):
    return list(df)


def get_merged_df_columns(df_left, df_right, **kwargs):
    return get_df_columns(merge_on_column(df_left, df_right, **kwargs))


def export_as_tsv(df, output_path):
    print("Exporting dataframe as TSV...")
    df.to_csv(output_path, sep="\t", index=False)


def read_tsv(input_path):
    print(f"Reading {input_path} as TSV")
    return pd.read_csv(input_path, sep="\t")


def read_chunk_tsv(input_path, chunksize=1_000_000):
    # DON'T USE IF YOU ARE PLANNING TO MERGE TABLES ON A KEY
    print(f"Reading {input_path} as TSV chunk")
    with pd.read_csv(input_path, sep="\t", chunksize=chunksize) as reader:
        yield from reader


def drop_columns(df, columns=[]):
    return df.drop(columns=columns)


def rename_column(df, from_colname, to_colname):
    return df.rename(columns={from_colname: to_colname})
