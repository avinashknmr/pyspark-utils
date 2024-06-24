from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer, expr

def flatten(df, columns=None):
    if columns is not None:
        df = df.select(*columns)

    flat_cols = []
    struct_cols = []
    array_cols = []

    for column in df.columns:
        if isinstance(df.schema[column].dataType, StructType):
            struct_cols.append(column)
        elif isinstance(df.schema[column].dataType, ArrayType):
            array_cols.append(column)
        else:
            flat_cols.append(column)
    
    if not (struct_cols + array_cols):
        return df
    
    struct_cols = [col(nc + '.' + c).alias(nc + '_' + c) for nc in struct_cols if isinstance(df.schema[nc].dataType, StructType) for c in df.select(nc+'.*').columns]
    flat_df = df.select(flat_cols + struct_cols + array_cols)
    exploded_cols = [(nc, explode_outer(nc).alias(nc)) for nc in array_cols if isinstance(df.schema[nc].dataType, ArrayType)]

    for original_col, explode_col in exploded_cols:
        flat_df = flat_df.withColumn(original_col, explode_col)

    return flatten(flat_df)

def pivot(df, array_col, pivot_col):
    main_cols = [c for c in df.columns if c != array_col]
    exploded_df = df.withColumn("assoc", explode_outer(col(array_col)))

    cols = [c for c in exploded_df.select("assoc.*").columns if c != pivot_col]

    pivot_exprs = []
    for c in cols:
        pivot_expr = expr(f"first(assoc.{c}) as {c}")
        pivot_exprs.append(pivot_expr)
        
    pivot_df = exploded_df.groupBy(*main_cols).pivot(f"assoc.{pivot_col}").agg(*pivot_exprs)
    return pivot_df
    

def rename_duplicate_columns():
    pass

def remove_duplicate_columns():
    pass

def column_map():
    pass