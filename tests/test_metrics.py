from src.quality.metrics import calculate_quality_metrics


def test_calculo_metricas_qualidade(spark):
    df_all = spark.range(10)
    df_valid = spark.range(8)
    df_invalid = spark.range(2)

    result = calculate_quality_metrics(
        df_all,
        df_valid,
        df_invalid,
    )

    assert result == {
        "total_records": 10,
        "valid_records": 8,
        "invalid_records": 2,
        "quality_rate": 80.0,
    }


def test_taxa_qualidade_com_arredondamento(spark):
    df_all = spark.range(3)
    df_valid = spark.range(2)
    df_invalid = spark.range(1)

    result = calculate_quality_metrics(
        df_all,
        df_valid,
        df_invalid,
    )

    assert result["quality_rate"] == 66.67


def test_metricas_dataset_totalmente_valido(spark):
    df_all = spark.range(5)
    df_valid = spark.range(5)
    df_invalid = spark.range(0)

    result = calculate_quality_metrics(
        df_all,
        df_valid,
        df_invalid,
    )

    assert result["total_records"] == 5
    assert result["valid_records"] == 5
    assert result["invalid_records"] == 0
    assert result["quality_rate"] == 100.0


def test_metricas_dataset_vazio(spark):
    df_all = spark.range(0)
    df_valid = spark.range(0)
    df_invalid = spark.range(0)

    result = calculate_quality_metrics(
        df_all,
        df_valid,
        df_invalid,
    )

    assert result == {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "quality_rate": 100.0,
    }