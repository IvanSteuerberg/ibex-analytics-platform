from pyspark.sql import SparkSession
from src.models.stock_data import StockDataModel
from src.views.console_view import ConsoleView
from src.config.paths import BRONZE_PATH, SENTIMIENTO_CSV_PATH

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

    def ejecutar_ej1b(self):
        df = self.stock_model.descargar_historico_total(start_date="2024-10-01", end_date="2026-10-01")
        df.write.mode("overwrite").parquet(BRONZE_PATH)

    def ejecutar_ej1c(self):
        df = self.spark.read.parquet(BRONZE_PATH)
        self.view.mostrar_esquema(df)
        self.view.mostrar_dataframe(df)

        df = self.spark.read.parquet(BRONZE_PATH).select("Ticker", "Date", "Close")

        self.view.mostrar_dataframe(df)
        self.view.mostrar_justificacion_select()

    def ejecutar_ej2a(self):
        # Guarda el fichero sentimiento_ibex.csv en un DataFrame de PySpark
        df = self.spark.read.option("header", True).csv(SENTIMIENTO_CSV_PATH)
        # Observa y comenta que todas las columnas llegan como texto
        self.view.mostrar_esquema(df)

    def ejecutar_ej2b(self):
        df = self.spark.read.option("header", True).csv(SENTIMIENTO_CSV_PATH)
        df.write.mode("overwrite").parquet(BRONZE_PATH + "/sentimiento")
        print("Se han escrito: ", df.count(), " filas")

    
    def ejecutar_ej2c(self):
        df = self.spark.read.parquet(BRONZE_PATH + "/sentimiento")
        rows_before = df.count()
        df.write.mode("append").parquet(BRONZE_PATH + "/sentimiento")
        rows_after = self.spark.read.parquet(BRONZE_PATH + "/sentimiento").count()
        print("Se han escrito: ", (rows_before - rows_after) ," filas, en modo append, debido a que a las 14652 añade otras 14652 en modo append.")
