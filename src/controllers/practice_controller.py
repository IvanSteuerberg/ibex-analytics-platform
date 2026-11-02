from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from src.models.stock_data import StockDataModel
from src.views.console_view import ConsoleView
from src.config.paths import BRONZE_PATH, SENTIMIENTO_CSV_PATH, BRONZE_PATH_SENTIMIENTO, BRONZE_PATH_IBEX
from datetime import date
from pyspark.errors import AnalysisException

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
        df.write.mode("overwrite").parquet(BRONZE_PATH_IBEX)

    def ejecutar_ej1c(self):
        df = self.spark.read.parquet(BRONZE_PATH_IBEX)
        self.view.mostrar_esquema(df)
        self.view.mostrar_dataframe(df)

        df = self.spark.read.parquet(BRONZE_PATH_IBEX).select("Ticker", "Date", "Close")

        self.view.mostrar_dataframe(df)
        self.view.mostrar_justificacion_select()

    def ejecutar_ej2a(self):
        # Guarda el fichero sentimiento_ibex.csv en un DataFrame de PySpark
        df = self.spark.read.option("header", True).option("sep", ";").csv(SENTIMIENTO_CSV_PATH)
        # Observa y comenta que todas las columnas llegan como texto
        self.view.mostrar_esquema(df)
        self.view.mostrar_justificacion_csv()

    def ejecutar_ej2b(self):
        df = self.spark.read.option("header", True).option("sep", ";").csv(SENTIMIENTO_CSV_PATH)
        df.write.mode("overwrite").parquet(BRONZE_PATH_SENTIMIENTO)
        print("Se han escrito: ", df.count(), " filas")

    
    def ejecutar_ej2c(self):
        rows_before = self.spark.read.parquet(BRONZE_PATH_SENTIMIENTO).count()
        df = self.spark.read.option("header", True).option("sep", ";").csv(SENTIMIENTO_CSV_PATH)
        df.write.mode("append").parquet(BRONZE_PATH_SENTIMIENTO)
        rows_after = self.spark.read.parquet(BRONZE_PATH_SENTIMIENTO).count()
        print(f"Filas antes: {rows_before} | Filas después: {rows_after}")
        print("Las filas se duplican: Parquet no detecta duplicados y append solo añade ficheros nuevos.")
        
    def ejecutar_ej3(self):
        DeltaTable.convertToDelta(self.spark, f"parquet.`{BRONZE_PATH_IBEX}`").history().select("version", "timestamp", "operation").show(truncate=False)
        DeltaTable.convertToDelta(self.spark, f"parquet.`{BRONZE_PATH_SENTIMIENTO}`").history().select("version", "timestamp", "operation").show(truncate=False)
        self.view.mostrar_justificacion_delta_log()

    def ejecutar_ej4(self):
        version = self.stock_model.obtener_version(BRONZE_PATH_IBEX)
        print(f"La versión actual del DeltaTable en {BRONZE_PATH_IBEX} es: {version}")
        filas_antes = self.spark.read.format("delta").load(BRONZE_PATH_IBEX).count()
        df = self.stock_model.descargar_historico_total(start_date="2024-10-01", end_date=date.today().isoformat())
        self.stock_model.merge_delta(df, BRONZE_PATH_IBEX)
        filas_despues = self.spark.read.format("delta").load(BRONZE_PATH_IBEX).count()
        print(f"Filas antes: {filas_antes} | Filas después: {filas_despues}")
        self.view.mostrar_justificacion_merge_delta()

    def ejecutar_ej5a(self):
        df = self.spark.read.format("delta").load(BRONZE_PATH_IBEX)
        df_nuevo = df.withColumn("Close_mas5", F.col("Close") + 5)
        try:
            df_nuevo.write.format("delta").mode("append").save(BRONZE_PATH_IBEX)
        except AnalysisException as e:
            print(str(e).split("\n")[0])

    def ejecutar_ej5b(self):
        df = self.spark.read.format("delta").load(BRONZE_PATH_IBEX)
        df_nuevo = df.withColumn("Close_mas5", F.col("Close") + 5)
        df_nuevo.write.format("delta").mode("append").option("mergeSchema", "true").save(BRONZE_PATH_IBEX)
        self.view.mostrar_justificacion_merge_schema()

    def ejecutar_ej5c(self):
        filas_antes = self.spark.read.format("delta").load(BRONZE_PATH_IBEX).count()
        self.stock_model.borrar_filas_ticker(BRONZE_PATH_IBEX, "BBVA.MC")
        filas_despues = self.spark.read.format("delta").load(BRONZE_PATH_IBEX).count()
        print(f"Filas antes: {filas_antes} | Filas después: {filas_despues}")

    def ejecutar_ej5d(self):
        # DeltaTable.forPath(self.spark, BRONZE_PATH_IBEX).history().show()
        # Formateado para poder leer algo
        (DeltaTable.forPath(self.spark, BRONZE_PATH_IBEX).history()
        .select("version", "timestamp", "operation").show(truncate=False))
        df_v1 = self.spark.read.format("delta").option("versionAsOf", 1).load(BRONZE_PATH_IBEX)
        df_actual = self.spark.read.format("delta").load(BRONZE_PATH_IBEX)
        print(f"Filas actual: {df_actual.count()} | Filas versión 1: {df_v1.count()}")

    def ejecutar_ej6(self):
        tabla = DeltaTable.forPath(self.spark, BRONZE_PATH_IBEX)
        tabla.restoreToVersion(1)
        (DeltaTable.forPath(self.spark, BRONZE_PATH_IBEX).history()
                .select("version", "timestamp", "operation").show(truncate=False))


