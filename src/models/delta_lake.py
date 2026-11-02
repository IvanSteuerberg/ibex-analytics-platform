from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


class DeltaLakeModel:
    """Modelo con las operaciones genéricas del data lake (Parquet y Delta), válidas para cualquier tabla."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # --- Parquet ---

    def leer_parquet(self, ruta: str) -> DataFrame:
        return self.spark.read.parquet(ruta)

    def escribir_parquet(self, df: DataFrame, ruta: str, modo: str = "overwrite"):
        df.write.mode(modo).parquet(ruta)

    # --- Delta ---

    def convertir_a_delta(self, ruta: str):
        """Convierte una carpeta Parquet en tabla Delta sin reescribir los datos (solo crea el _delta_log)."""
        DeltaTable.convertToDelta(self.spark, f"parquet.`{ruta}`")

    def leer_delta(self, ruta: str, version: int = None) -> DataFrame:
        """Lee la tabla Delta. Si se indica version, lee esa versión antigua (time travel) sin modificar la tabla."""
        lector = self.spark.read.format("delta")
        if version is not None:
            lector = lector.option("versionAsOf", version)
        return lector.load(ruta)

    def contar_filas(self, ruta: str) -> int:
        return self.leer_delta(ruta).count()

    def escribir_delta(self, df: DataFrame, ruta: str, modo: str = "append", merge_schema: bool = False):
        """Escribe en Delta. Con merge_schema=True permite añadir columnas nuevas (schema evolution)."""
        escritor = df.write.format("delta").mode(modo)
        if merge_schema:
            escritor = escritor.option("mergeSchema", "true")
        escritor.save(ruta)

    def merge_insertar_nuevos(self, nuevos: DataFrame, ruta: str, claves: list):
        """MERGE que inserta solo las filas cuya clave no existe en la tabla (carga idempotente)."""
        condicion = " AND ".join(f"d.{c} = n.{c}" for c in claves)
        destino = DeltaTable.forPath(self.spark, ruta)
        (destino.alias("d").merge(nuevos.alias("n"), condicion)
            .whenNotMatchedInsertAll()
            .execute())

    def borrar_filas(self, ruta: str, condicion: str):
        DeltaTable.forPath(self.spark, ruta).delete(condicion)

    def obtener_version(self, ruta: str) -> int:
        return DeltaTable.forPath(self.spark, ruta).history(1).select("version").collect()[0]["version"]

    def historial(self, ruta: str) -> DataFrame:
        """Historial de versiones, solo con las columnas legibles."""
        return DeltaTable.forPath(self.spark, ruta).history().select("version", "timestamp", "operation")

    def restaurar_version(self, ruta: str, version: int):
        """Deja la tabla como estaba en esa versión (crea un commit nuevo, no borra el historial)."""
        DeltaTable.forPath(self.spark, ruta).restoreToVersion(version)
