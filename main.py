from src.config.spark_session import get_spark_session
from src.controllers.practice_controller import PracticeController


def main():
    spark = get_spark_session()
    controller = PracticeController(spark)

    # ===== CLASE 1 =====
    # Bronze en Parquet. Ojo: Ej1b, Ej2b y Ej2c escriben Parquet; si bronze ya es Delta (Ej3),
    # borra data/lake/bronze antes de volver a ejecutarlos.

    # Ej1-a
    # print("Ej1-a")
    # controller.ejecutar_ej1a()

    # Ej1-b
    # print("Ej1-b")
    # controller.ejecutar_ej1b()

    # Ej1-c
    # print("Ej1-c")
    # controller.ejecutar_ej1c()

    # Ej2-a
    # print("Ej2-a")
    # controller.ejecutar_ej2a()

    # Ej2-b
    # print("Ej2-b")
    # controller.ejecutar_ej2b()

    # Ej2-c
    # print("Ej2-c")
    # controller.ejecutar_ej2c()

    # ===== CLASE 2 =====
    # Delta sobre bronze/ibex. Ej5c, Ej5d y Ej6 modifican la tabla en cada ejecución.

    # Ej3
    # print("Ej3")
    # controller.ejecutar_ej3()

    # Ej4
    # print("Ej4")
    # controller.ejecutar_ej4()

    # Ej5-a
    # print("Ej5-a")
    # controller.ejecutar_ej5a()

    # Ej5-b
    # print("Ej5-b")
    # controller.ejecutar_ej5b()

    # Ej5-c
    # print("Ej5-c")
    # controller.ejecutar_ej5c()

    # Ej5-d
    # print("Ej5-d")
    # controller.ejecutar_ej5d()

    # Ej6
    # print("Ej6")
    # controller.ejecutar_ej6()

    # Finalizar la sesión de Spark
    spark.stop()


if __name__ == "__main__":
    main()
