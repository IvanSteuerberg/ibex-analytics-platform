# 📌 Normativa de Entrega y Código (Prácticas)

## 1. Formato y Empaquetado
* **Nombre del archivo:** La entrega debe ser un archivo `.zip` nombrado estrictamente como `NombreDeUsuarioUle_practica1.zip`.
* **Independencia del entorno:** El proyecto debe poder ejecutarse en cualquier ordenador. **Están prohibidas las rutas absolutas**.
* **Contenido:** Hay que entregar el proyecto completo y funcionando, con el código ordenado y bien estructurado.
* **Recursos:** Se deben incluir todos los archivos necesarios (como los `.csv`). No se incluye la base de datos SQL, pero sí el código para conectarse a ella.
* **Extensión:** Todos los archivos que contengan código deben terminar en `.py`.

## 2. Arquitectura y Limpieza (Patrón MVC)
* **Organización MVC:** El proyecto debe seguir el patrón Modelo-Vista-Controlador. La lógica de la aplicación debe estar dividida en clases y directorios claros para que sea fácil de entender, mantener y escalar.
* **Separación de responsabilidades:**
  * **Inicialización de Spark:** Debe existir un módulo aparte (ej. `spark_session.py`) que defina la función para crear la `SparkSession`. Esto centraliza la configuración y evita ensuciar la lógica principal.
  * **Conexión a Base de Datos:** Debe existir otro módulo dedicado exclusivamente a gestionar la conexión a la base de datos utilizando JDBC.
* **Etiquetado de ejercicios:** Es obligatorio indicar claramente a qué ejercicio pertenece cada sección del código.
* **Formato de etiqueta:** Cada bloque debe ir precedido por su comentario y su impresión por pantalla exacta:

  ```python
  # Ej1-a
  print("Ej1-a")
  # TODO: tu código aquí
  ```

## 3. Salidas por Consola (Outputs)
* Estricto cumplimiento: Solo se debe imprimir la información solicitada ("muestra"). No mostrar información adicional.
* Respuestas teóricas: Cuando un ejercicio requiera comentar, reflexionar o investigar, se debe hacer imprimiendo texto por consola:
    ```python
    # Ej2-b
    print("Ej2-b")
    print("Comentario/Reflexion/Investigación")
    print("Referencia/s")
    ```