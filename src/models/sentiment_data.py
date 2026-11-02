from pyspark.sql import DataFrame, SparkSession
from src.config.paths import SENTIMIENTO_CSV_PATH


class SentimentDataModel:
    """Modelo responsable de la ingesta y limpieza del índice de sentimiento por sector."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def leer_csv(self, ruta: str = SENTIMIENTO_CSV_PATH) -> DataFrame:
        """Lee el CSV crudo: separador ';' y cabecera. Todas las columnas llegan como string."""
        return self.spark.read.option("header", True).option("sep", ";").csv(ruta)
