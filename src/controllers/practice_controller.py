from datetime import date
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from src.models.stock_data import StockDataModel
from src.models.sentiment_data import SentimentDataModel
from src.models.delta_lake import DeltaLakeModel
from src.models.gold_data import GoldDataModel
from src.views.console_view import ConsoleView
from src.config.paths import *

# Clave que identifica una cotización: una empresa en un día
CLAVE_PRECIOS = ["Ticker", "Date"]
# Versión de bronze/ibex tras el Ej4 (0 = CONVERT del Ej3, 1 = MERGE del Ej4)
VERSION_EJ4 = 1


class PracticeController:
    """Controlador que orquesta los modelos y las vistas para los ejercicios."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.stock_model = StockDataModel(spark)
        self.sentiment_model = SentimentDataModel(spark)
        self.lake = DeltaLakeModel(spark)
        self.view = ConsoleView()
        self.gold_model = GoldDataModel(spark)

    def ejecutar_ej1a(self):
        df = self.stock_model.descargar_historico(ticker="BBVA.MC", start_date="2021-10-01", end_date="2026-10-01")
        self.view.mostrar_dataframe(df)
        self.view.mostrar_esquema(df)
        self.view.mostrar_justificacion_nulos()

    def ejecutar_ej1b(self):
        df = self.stock_model.descargar_historico_total(start_date="2024-10-01", end_date="2026-10-01")
        self.lake.escribir_parquet(df, BRONZE_PATH_IBEX)

    def ejecutar_ej1c(self):
        df = self.lake.leer_parquet(BRONZE_PATH_IBEX)
        self.view.mostrar_esquema(df)
        self.view.mostrar_dataframe(df)

        df = self.lake.leer_parquet(BRONZE_PATH_IBEX).select("Ticker", "Date", "Close")
        self.view.mostrar_dataframe(df)
        self.view.mostrar_justificacion_select()

    def ejecutar_ej2a(self):
        df = self.sentiment_model.leer_csv()
        self.view.mostrar_esquema(df)
        self.view.mostrar_justificacion_csv()

    def ejecutar_ej2b(self):
        df = self.sentiment_model.leer_csv()
        self.lake.escribir_parquet(df, BRONZE_PATH_SENTIMIENTO)
        self.view.mostrar_filas_escritas(df.count())

    def ejecutar_ej2c(self):
        antes = self.lake.leer_parquet(BRONZE_PATH_SENTIMIENTO).count()
        df = self.sentiment_model.leer_csv()
        self.lake.escribir_parquet(df, BRONZE_PATH_SENTIMIENTO, modo="append")
        despues = self.lake.leer_parquet(BRONZE_PATH_SENTIMIENTO).count()
        self.view.mostrar_antes_despues(antes, despues)
        self.view.mostrar_justificacion_append_parquet()

    def ejecutar_ej3(self):
        for ruta in (BRONZE_PATH_IBEX, BRONZE_PATH_SENTIMIENTO):
            self.lake.convertir_a_delta(ruta)
            self.view.mostrar_historial(self.lake.historial(ruta))
        self.view.mostrar_justificacion_delta_log()

    def ejecutar_ej4(self):
        self.view.mostrar_version(self.lake.obtener_version(BRONZE_PATH_IBEX))
        antes = self.lake.contar_filas(BRONZE_PATH_IBEX)
        df = self.stock_model.descargar_historico_total(start_date="2024-10-01", end_date=date.today().isoformat())
        self.lake.merge_insertar_nuevos(df, BRONZE_PATH_IBEX, CLAVE_PRECIOS)
        despues = self.lake.contar_filas(BRONZE_PATH_IBEX)
        self.view.mostrar_antes_despues(antes, despues)
        self.view.mostrar_justificacion_merge_delta()

    def ejecutar_ej5a(self):
        df = self.stock_model.anadir_close_mas5(self.lake.leer_delta(BRONZE_PATH_IBEX))
        try:
            self.lake.escribir_delta(df, BRONZE_PATH_IBEX)
        except AnalysisException as e:
            self.view.mostrar_error(e)

    def ejecutar_ej5b(self):
        df = self.stock_model.anadir_close_mas5(self.lake.leer_delta(BRONZE_PATH_IBEX))
        self.lake.escribir_delta(df, BRONZE_PATH_IBEX, merge_schema=True)
        self.view.mostrar_justificacion_merge_schema()

    def ejecutar_ej5c(self):
        antes = self.lake.contar_filas(BRONZE_PATH_IBEX)
        self.lake.borrar_filas(BRONZE_PATH_IBEX, "Ticker = 'BBVA.MC'")
        despues = self.lake.contar_filas(BRONZE_PATH_IBEX)
        self.view.mostrar_antes_despues(antes, despues)

    def ejecutar_ej5d(self):
        self.view.mostrar_historial(self.lake.historial(BRONZE_PATH_IBEX))
        # La versión de antes del Ej5a es la del Ej4 (el Ej5a falló y no dejó versión)
        filas_v_ej4 = self.lake.leer_delta(BRONZE_PATH_IBEX, version=VERSION_EJ4).count()
        filas_actual = self.lake.contar_filas(BRONZE_PATH_IBEX)
        self.view.mostrar_comparacion_versiones(filas_actual, filas_v_ej4, VERSION_EJ4)
        self.view.mostrar_justificacion_version_antigua()

    def ejecutar_ej6(self):
        self.lake.restaurar_version(BRONZE_PATH_IBEX, VERSION_EJ4)
        self.view.mostrar_historial(self.lake.historial(BRONZE_PATH_IBEX))
        self.view.mostrar_filas_actuales(self.lake.contar_filas(BRONZE_PATH_IBEX))

    def ejecutar_ej7(self):
        df_info = self.stock_model.descargar_info_empresas()
        self.view.mostrar_dataframe(df_info.select("Ticker", "Nombre", "Sector", "Industria"), num_filas=36)

        df_sentimiento = self.sentiment_model.leer_csv()
        sectores_csv = df_sentimiento.select("Sector").distinct().count()
        sectores_normalizados = self.sentiment_model.normalizar_sector(df_sentimiento).select("Sector").distinct().count()
        self.view.mostrar_sectores_csv(sectores_csv, sectores_normalizados)

        comparacion = self.sentiment_model.comparar_con_empresas(df_sentimiento, df_info)
        self.view.mostrar_dataframe(comparacion, num_filas=20)
        self.view.mostrar_reflexion_yahoo()

    def ejecutar_ej8(self):
        df_bronze = self.lake.leer_delta(BRONZE_PATH_SENTIMIENTO)
        df_silver = self.sentiment_model.limpiar_sentimiento(df_bronze)
        self.lake.escribir_delta(df_silver, SILVER_PATH_SENTIMIENTO, "overwrite")
        filas_bronze = self.lake.contar_filas(BRONZE_PATH_SENTIMIENTO)
        filas_silver = self.lake.contar_filas(SILVER_PATH_SENTIMIENTO)
        self.view.mostrar_filas_bronze_silver(filas_bronze, filas_silver)
        self.view.mostrar_esquema(df_silver)

    def ejecutar_ej9(self):
        # IBEX leer bronze, limpiar y guardar silver
        df_ibex_bronze = self.lake.leer_delta(BRONZE_PATH_IBEX)
        df_ibex_silver = self.stock_model.limpiar_ibex(df_ibex_bronze)
        self.lake.escribir_delta(df_ibex_silver, SILVER_PATH_IBEX, "overwrite")

        # EMPRESA descargar y guardar bronze, leer bronze, limpiar y guardar silver
        df_empresa_bronze = self.stock_model.descargar_info_empresas()
        self.lake.escribir_delta(df_empresa_bronze, BRONZE_PATH_EMPRESA, "overwrite")
        df_empresa_bronze = self.lake.leer_delta(BRONZE_PATH_EMPRESA)
        df_empresa_silver = self.stock_model.limpiar_info_empresas(df_empresa_bronze)
        self.lake.escribir_delta(df_empresa_silver, SILVER_PATH_EMPRESA, "overwrite")

        # Mostrar filas
        self.view.mostrar_dataframe(df_ibex_silver, truncar=True)
        self.view.mostrar_dataframe(df_empresa_silver, truncar=True)

    def ejecutar_ej10(self):
        df_ibex = self.lake.leer_delta(SILVER_PATH_IBEX)
        df_empresa = self.lake.leer_delta(SILVER_PATH_EMPRESA)
        df_sentimiento = self.lake.leer_delta(SILVER_PATH_SENTIMIENTO)

        hecho = self.gold_model.construir_hecho(df_ibex, df_empresa, df_sentimiento)
        dim_empresa = self.gold_model.construir_dim_empresa(df_empresa)
        dim_sector = self.gold_model.construir_dim_sector(df_empresa)
        dim_fecha = self.gold_model.construir_dim_fecha(df_ibex)

        self.lake.escribir_delta(hecho, GOLD_PATH_HECHO, "overwrite")
        self.lake.escribir_delta(dim_empresa, GOLD_PATH_DIM_EMPRESA, "overwrite")
        self.lake.escribir_delta(dim_sector, GOLD_PATH_DIM_SECTOR, "overwrite")
        self.lake.escribir_delta(dim_fecha, GOLD_PATH_DIM_FECHA, "overwrite")

        self.view.mostrar_esquema(hecho)
        self.view.mostrar_dataframe(hecho, truncar=True)
        filas_silver = self.lake.contar_filas(SILVER_PATH_IBEX)
        filas_hecho = self.lake.contar_filas(GOLD_PATH_HECHO)
        self.view.mostrar_antes_despues(filas_silver, filas_hecho)