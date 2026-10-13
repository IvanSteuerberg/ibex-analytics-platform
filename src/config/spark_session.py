import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str = "IBEX35") -> SparkSession:
    """
    Inicializa y devuelve una sesión de Spark configurada para Delta Lake
    y optimizada para su ejecución local.
    """
    # Configuración de ejecutables de Python para evitar errores de workers en Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Detección de Java en el entorno Conda si no está en variables de entorno
    if "JAVA_HOME" not in os.environ:
        conda_java = os.path.join(sys.prefix, "Library")
        if os.path.exists(os.path.join(conda_java, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = conda_java

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
