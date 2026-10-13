from pyspark.sql import SparkSession
from src.models.stock_data import StockDataModel
from src.views.console_view import ConsoleView


class PracticeController:
    """Controlador que orquesta los modelos y las vistas para los ejercicios."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.stock_model = StockDataModel(spark)
        self.view = ConsoleView()

    def ejecutar_ej1a(self):
        """Orquesta la ejecución del Ejercicio 1-a."""
        df = self.stock_model.descargar_historico(ticker="BBVA.MC", start_date="2021-10-01", end_date="2026-10-01")
        self.view.mostrar_dataframe(df)
        self.view.mostrar_esquema(df)
        self.view.mostrar_justificacion_nulos()
