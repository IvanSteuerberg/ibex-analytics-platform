# IBEX-35 Data System

**Autores:** Iván Rodríguez Steuerberg y Álvaro Rodríguez Pino

Este repositorio contiene un sistema de datos completo sobre el dominio de la bolsa española. La aplicación se construye progresivamente para procesar y analizar la volatilidad y evolución del índice IBEX 35, con el fin de identificar tendencias bursátiles.

## Estructura del Data Lake
El proyecto implementa un pipeline de datos bajo la arquitectura Medallion (capas Bronze, Silver y Gold) utilizando el formato Delta Lake. El flujo de datos limpia, integra y cruza dos fuentes principales:
*   Precios diarios de cotización extraídos de Yahoo Finance (`yfinance`).
*   Índice de sentimiento de mercado estructurado por sector (archivo CSV).

## Stack Tecnológico
*   **Entorno:** Python 3.11.
*   **Procesamiento:** Apache Spark 4.x (PySpark en modo local).
*   **Almacenamiento:** Delta Lake.
*   **Características:** Soporte para transacciones ACID, control de versiones (Time Travel) y operaciones como MERGE y DELETE.