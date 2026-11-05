from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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

    def add_sector(self, df_ibex: DataFrame, df_empresa: DataFrame) -> DataFrame:
        return df_ibex.join(df_empresa.select("Ticker", "Sector"), on="Ticker", how="Left")

    def precios_sin_sentimiento(self, df_ibex: DataFrame, df_empresa: DataFrame, df_sentimiento: DataFrame) -> DataFrame:
        precios = self.add_sector(df_ibex, df_empresa)
        sentimiento = df_sentimiento.withColumnRenamed("Fecha", "Date")
        return precios.join(sentimiento, on=["Sector", "Date"], how="left_anti")

    def sentimiento_sin_precios(self, df_ibex: DataFrame, df_empresa: DataFrame, df_sentimiento: DataFrame) -> DataFrame:
        precios = self.add_sector(df_ibex, df_empresa).select("Sector", "Date").dropDuplicates()
        sentimiento = df_sentimiento.withColumnRenamed("Fecha", "Date")
        return sentimiento.join(precios, on=["Sector", "Date"], how="left_anti")

    def anadir_metricas(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("Rango_Dia", F.col("High") - F.col("Low"))
        df = df.withColumn("Variacion_Intradia_Pct", (F.col("Close") - F.col("Open")) / F.col("Open") * 100)
        return df