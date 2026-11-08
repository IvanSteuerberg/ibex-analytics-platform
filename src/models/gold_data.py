from pyspark.sql import DataFrame, SparkSession, Window
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
        """Una fila por día de cotización, con los atributos para filtrar y agrupar en Power BI."""
        return (df_ibex.select("Date").dropDuplicates()
                .withColumn("Anio", F.year("Date"))
                .withColumn("Trimestre", F.quarter("Date"))
                .withColumn("Mes", F.month("Date"))
                .withColumn("Dia_Semana", F.date_format("Date", "EEEE"))
                .orderBy("Date"))

    def construir_hecho(self, df_ibex: DataFrame, df_empresa: DataFrame, df_sentimiento: DataFrame) -> DataFrame:
        sentimiento = df_sentimiento.withColumnRenamed("Fecha", "Date")
        return self.add_sector(df_ibex, df_empresa).join(sentimiento, on=["Sector", "Date"], how="Left")

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

    def resumir_por_fecha(self, df: DataFrame) -> DataFrame:
        """Cuántas filas hay de cada fecha, ordenadas."""
        return df.groupBy("Date").count().orderBy("Date")

    def resumir_por_sector(self, df: DataFrame) -> DataFrame:
        """Por cada sector: cuántas filas hay y entre qué fechas."""
        return (df.groupBy("Sector")
                .agg(F.count("*").alias("Filas"), F.min("Date").alias("Desde"), F.max("Date").alias("Hasta"))
                .orderBy("Sector"))

    def anadir_metricas(self, df: DataFrame) -> DataFrame:
        """Medidas para Power BI: rango del día, variación intradía y retorno diario (todo en %)."""
        por_empresa = Window.partitionBy("Ticker").orderBy("Date")
        df = df.withColumn("Rango_Dia_Pct", (F.col("High") - F.col("Low")) / F.col("Open") * 100)
        df = df.withColumn("Variacion_Intradia_Pct", (F.col("Close") - F.col("Open")) / F.col("Open") * 100)
        df = df.withColumn("Retorno_Diario_Pct",
                           (F.col("Adj_Close") / F.lag("Adj_Close").over(por_empresa) - 1) * 100)
        return df