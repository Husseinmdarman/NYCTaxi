def check_nulls(df , columns: list) -> dict:
    """Checks for null values in specified columns of a DataFrame."""
    
    results = {}
    for column in columns:
         results[column] = df.filter(df[column].isNull()).count()
    return results

def validate_schema(df, expected_schema) -> list:
    """Validates the schema of a DataFrame against an expected schema."""
    
    df_cols = set(df.columns)
    expected_cols = set(expected_schema.fieldNames())
    missing_cols = expected_cols - df_cols
    extra_cols = df_cols - expected_cols
    return {
        "missing_columns": list(missing_cols),
        "extra_columns": list(extra_cols),
        "passed": len(missing_cols) == 0
    }
    