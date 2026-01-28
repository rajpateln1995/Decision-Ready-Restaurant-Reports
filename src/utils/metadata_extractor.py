import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Tuple

def normalize_table_name(file_name: str) -> str:
    """Normalize file name to a logical table name (no extension, snake_case)"""
    base = file_name.rsplit('.', 1)[0]
    return re.sub(r'[^a-zA-Z0-9]+', '_', base).lower()

def get_column_metadata(df: pd.DataFrame) -> List[Dict[str, Any]]:
    col_meta = []
    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        # Map pandas dtype to LLM-friendly type
        if pd.api.types.is_integer_dtype(series):
            llm_type = 'int'
        elif pd.api.types.is_float_dtype(series):
            llm_type = 'float'
        elif pd.api.types.is_datetime64_any_dtype(series):
            llm_type = 'datetime'
        else:
            llm_type = 'string'
        nullable = series.isnull().any()
        unique_count = series.nunique(dropna=True)
        sample_values = series.dropna().unique()[:5].tolist()
        min_val, max_val = None, None
        if llm_type in ['int', 'float', 'datetime']:
            if not series.dropna().empty:
                min_val = series.min()
                max_val = series.max()
        distinct_value_ratio = unique_count / len(series) if len(series) > 0 else 0
        col_meta.append({
            'column_name': col,
            'data_type': llm_type,
            'nullable': bool(nullable),
            'unique_count': int(unique_count),
            'sample_values': sample_values,
            'min_value': min_val,
            'max_value': max_val,
            'distinct_value_ratio': distinct_value_ratio
        })
    return col_meta

def get_file_metadata(file_name: str, df: pd.DataFrame) -> Dict[str, Any]:
    return {
        'file_name': file_name,
        'table_name': normalize_table_name(file_name),
        'row_count': int(len(df)),
        'column_count': int(len(df.columns)),
        'columns': get_column_metadata(df)
    }

def get_sample_data(df: pd.DataFrame, n_head: int = 5, n_tail: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    head = df.head(n_head).to_dict(orient='records')
    tail = df.tail(n_tail).to_dict(orient='records')
    return {'head': head, 'tail': tail}

def find_primary_key_candidates(df: pd.DataFrame) -> List[str]:
    candidates = []
    for col in df.columns:
        if df[col].is_unique and not df[col].isnull().any():
            candidates.append(col)
    return candidates

def find_foreign_key_candidates(dfs: Dict[str, pd.DataFrame]) -> Dict[str, List[Tuple[str, str]]]:
    # For each table, for each column, see if it is a subset of another table's column
    fk_candidates = {}
    for t1, df1 in dfs.items():
        fk_candidates[t1] = []
        for col1 in df1.columns:
            vals1 = set(df1[col1].dropna().unique())
            if len(vals1) == 0:
                continue
            for t2, df2 in dfs.items():
                if t1 == t2:
                    continue
                for col2 in df2.columns:
                    vals2 = set(df2[col2].dropna().unique())
                    if len(vals1) > 0 and len(vals2) > 0 and vals1.issubset(vals2) and len(vals1) < len(vals2):
                        fk_candidates[t1].append((col1, f"{t2}.{col2}"))
    return fk_candidates

def find_joinable_columns(dfs: Dict[str, pd.DataFrame]) -> List[Tuple[str, str, str]]:
    # Find columns with the same name and compatible types across tables
    joinable = []
    for t1, df1 in dfs.items():
        for t2, df2 in dfs.items():
            if t1 >= t2:
                continue
            for col in set(df1.columns).intersection(df2.columns):
                # Check type compatibility
                if str(df1[col].dtype) == str(df2[col].dtype):
                    joinable.append((f"{t1}.{col}", f"{t2}.{col}", str(df1[col].dtype)))
    return joinable

def extract_csvs_metadata(file_dfs: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    file_dfs: {file_name: DataFrame}
    Returns dict with all metadata for LLM context.
    """
    tables = {}
    for file_name, df in file_dfs.items():
        meta = get_file_metadata(file_name, df)
        meta['sample_data'] = get_sample_data(df)
        meta['primary_key_candidates'] = find_primary_key_candidates(df)
        tables[meta['table_name']] = meta
    # Relationship metadata
    fk_candidates = find_foreign_key_candidates({k: v for k, v in file_dfs.items()})
    joinable = find_joinable_columns({k: v for k, v in file_dfs.items()})
    return {
        'tables': tables,
        'foreign_key_candidates': fk_candidates,
        'joinable_columns': joinable
    }

