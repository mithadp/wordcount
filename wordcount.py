from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Inisialisasi Spark Session
spark = SparkSession.builder.appName("Word Count").getOrCreate()

# Step 1: Data (Kalimat Sederhana)
data = [
    ("Hello world this is Spark",),
    ("Spark makes big data processing simple",),
    ("Hello Spark",)
]

# Membuat DataFrame dari kalimat
schema = ["sentence"]
df = spark.createDataFrame(data, schema=schema)

# Step 2: Transformasi Data
word_counts = (
    df
    .withColumn("word", explode(split(col("sentence"), " ")))
    .groupBy("word")
    .count()
    .orderBy("count", ascending=False)
)

# Step 3: Simpan Hasil
output_path = "/opt/spark-apps/output/wordcount.txt"
word_counts.write.csv(output_path, header=True, mode="overwrite")

# Tampilkan hasil di console
word_counts.show()

# Stop Spark Session
spark.stop()
