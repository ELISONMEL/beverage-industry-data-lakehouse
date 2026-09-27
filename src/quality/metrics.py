def calculate_quality_metrics(df_all, df_valid, df_invalid):
    total = df_all.count()
    valid = df_valid.count()
    invalid = df_invalid.count()
    quality_rate = (valid / total * 100) if total else 100.0
    return {
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "quality_rate": round(quality_rate, 2),
    }
