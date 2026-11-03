from pyspark.sql import DataFrame, SparkSession, functions as F
from src.config.paths import SENTIMIENTO_CSV_PATH


class SentimentDataModel:
    """Modelo responsable de la ingesta y limpieza del índice de sentimiento por sector."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def leer_csv(self, ruta: str = SENTIMIENTO_CSV_PATH) -> DataFrame:
        """Lee el CSV crudo: separador ';' y cabecera. Todas las columnas llegan como string."""
        return self.spark.read.option("header", True).option("sep", ";").csv(ruta)

    def normalizar_sector(self, df: DataFrame) -> DataFrame:
        """Unifica el nombre del sector: quita espacios y deja mayúscula inicial ('  ENERGY ' -> 'Energy')."""
        return df.withColumn("Sector", F.initcap(F.trim(F.col("Sector"))))

    def comparar_con_empresas(self, df_sentimiento: DataFrame, df_info: DataFrame) -> DataFrame:
        """
        Por cada sector: cuántas empresas del IBEX tiene según Yahoo y qué días cubre el sentimiento.
        Usa un outer join para que aparezcan también los sectores que solo están en una de las dos fuentes.
        """
        empresas = df_info.groupBy("Sector").agg(F.count("*").alias("Empresas"))
        sentimiento = (self.normalizar_sector(df_sentimiento)
                       .groupBy("Sector")
                       .agg(F.count("*").alias("Dias_sentimiento"),
                            F.min("Fecha").alias("Desde"),
                            F.max("Fecha").alias("Hasta")))
        return (sentimiento.join(empresas, on="Sector", how="outer")
                .fillna(0, subset=["Empresas"])
                .orderBy(F.desc("Empresas")))

    def limpiar_sentimiento(self, df: DataFrame):
        df = self.normalizar_sector(df)
        df = df.withColumn("Fecha", F.to_date(F.col("Fecha")))
        df = df.withColumn("Sentimiento", F.regexp_replace(F.col("Sentimiento"), ",", "."))
        df = df.withColumn("Sentimiento", F.col("Sentimiento").cast("double"))
        df = df.dropna()
        df = df.dropDuplicates(["Fecha", "Sector"])
        return df

