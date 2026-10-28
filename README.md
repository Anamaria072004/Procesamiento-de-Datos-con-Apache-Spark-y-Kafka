# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Universidad Nacional Abierta y a Distancia (UNAD)**
**Curso:** Big Data (202016911)
**Estudiante:** Ana María Acevedo Moreno
**Grupo:** 202016911_67

## Descripción de la Solución
Este repositorio contiene la implementación de una solución de procesamiento de datos en tiempo real y por lotes (batch) utilizando Apache Spark y Apache Kafka. 

**Caso de uso:** Monitoreo de sensores IoT. Se simula la llegada de datos de temperatura y humedad de 10 sensores diferentes. Los datos son enviados a Kafka y procesados en tiempo real por Spark Streaming para calcular promedios en ventanas de 1 minuto.

## Estructura del Proyecto
- `productor_sensores.py`: Script que simula los sensores y envía datos en formato JSON al topic `sensor_data` de Kafka.

- `spark_streaming_consumer.py`: Script de Spark Streaming que lee de Kafka, parsea el JSON y calcula promedios usando ventanas de tiempo.

- `batch_analysis.py`: Script de Spark que realiza un análisis exploratorio (EDA) sobre datos históricos.

- `requisitos.txt`: Dependencias necesarias para ejecutar el proyecto.

## Requisitos e Instalación
1. Tener instalado **Apache Kafka** (se usó la versión 3.7.0) y **Apache Spark** (versión 3.5.3).
2. Tener Python 3.10 o superior.
3. Instalar las dependencias:
   ```bash
   pip install -r requisitos.txt


---------------------------------------
## Instrucciones de Ejecución
1. Iniciar los servidores de Kafka
Abrir dos terminales y ejecutar:


# Terminal 1 (ZooKeeper)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 (Kafka Broker)
bin/kafka-server-start.sh config/server.properties


2. Crear el Topic

in/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data

3. Ejecutar el Productor (Simulador de Sensores)
bash

python3 productor_sensores.py

4. Ejecutar el Consumidor de Spark Streaming
En una nueva terminal:

bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py

-----------------------------------------
 Código del Productor (productor_sensores.py):A continuación se presenta el código utilizado para simular los sensores IoT y enviar los datos a Kafka:

python
import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=("localhost:9092",),
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

while True:
    sensor_data = dict(
        sensor_id=random.randint(1, 10),
        temperature=round(random.uniform(20, 30), 2),
        humidity=round(random.uniform(30, 70), 2),
        timestamp=int(time.time())
    )
    producer.send('sensor_data', value=sensor_data)
    print("Sent:", sensor_data)
    time.sleep(1)

