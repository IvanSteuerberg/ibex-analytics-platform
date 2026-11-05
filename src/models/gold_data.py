from pyspark.sql import DataFrame, SparkSession

class GoldDataModel:
    """Modelo para construir el modelo en estrella de la capa gold"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def construir_dim_empresa(self, df_empresa: DataFrame) -> DataFrame:
        return df_empresa.drop_duplicates(["Ticker"])

    def construir_dim_sector(self, df_empresa: DataFrame) -> DataFrame:
        return df_empresa.select("Sector").dropDuplicates()

    def construir_dim_fecha(self, df_ibex: DataFrame) -> DataFrame:
        return df_ibex.select("Date").dropDuplicates()

    def construir_hecho(self, df_ibex: DataFrame, df_empresa: DataFrame, df_sentimiento: DataFrame) -> DataFrame:
        sector_por_ticker = df_empresa.select("Ticker", "Sector")
        sentimiento = df_sentimiento.withColumnRenamed("Fecha", "Date")

        return (df_ibex
                .join(sector_por_ticker, on="Ticker", how="Left")
                .join(sentimiento, on=["Sector", "Date"], how="Left")
        )