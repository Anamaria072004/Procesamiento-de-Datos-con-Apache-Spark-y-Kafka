from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, round

spark = SparkSession.builder \
    .appName("BatchSensorAnalysis") \
    .getOrCreate()

data = (
    (1, 25.5, 50.0), 
    (2, 26.0, 55.0), 
    (1, 24.0, 45.0), 
    (3, 30.0, 60.0), 
    (2, 27.0, 52.0)
)
columns = ("sensor_id", "temperature", "humidity")

df = spark.createDataFrame(data, columns)

stats = df.groupBy("sensor_id").agg(
    round(avg("temperature"), 2).alias("avg_temp"),
    round(max("temperature"), 2).alias("max_temp"),
    round(min("temperature"), 2).alias("min_temp"),
    round(avg("humidity"), 2).alias("avg_hum"),
    count("*").alias("total_registros")
)

print("Resultados del Análisis Batch:")
stats.show()
spark.stop()