"""
This script is a Kafka consumer for the YouTube ETL pipeline.
It reads video and channel data from the Kafka topic 'youtube_data'.
The records are parsed and processed using PySpark.

Each record is validated against a defined schema (channel/video).
The structured data is stored in HDFS as Parquet files.
This setup enables distributed processing and scalable analytics.
"""

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
import json

spark = SparkSession.builder.appName("YouTubeKafkaConsumer").getOrCreate()

KAFKA_TOPIC = "youtube_data"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='youtube_consumer_group'
)

channel_schema = StructType([
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("subscriber_count", LongType(), True),
    StructField("views_count", LongType(), True),
    StructField("total_videos", LongType(), True),
    StructField("playlist_id", StringType(), True)
])

video_schema = StructType([
    StructField("playlist_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("publish_at", StringType(), True),
    StructField("views_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("video_duration", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("caption", StringType(), True),
    StructField("definition", StringType(), True)
])

HDFS_CHANNEL_PATH = "hdfs://localhost:9000/user/hadoop/youtube_data/channel_stats"
HDFS_VIDEO_PATH = "hdfs://localhost:9000/user/hadoop/youtube_data/video_stats"

def process_message(message):
     """
    Processes one message from Kafka containing either video or channel data.
    Converts it to a Spark DataFrame using the appropriate schema.
    Writes the data into HDFS as Parquet for long-term storage.
    """
    data = message.value
    if data["type"] == "channel":
        try:
            for k in ["subscriber_count", "views_count", "total_videos"]:
                data["data"][k] = int(data["data"].get(k, 0))
            df = spark.createDataFrame([data["data"]], schema=channel_schema)
            df.write.mode("append").parquet(HDFS_CHANNEL_PATH)
            print(f"Stored channel: {data['data']['channel_name']}")
        except Exception as e:
            print(f"[ERROR] Channel store failed: {e}")
    elif data["type"] == "video":
        try:
            for k in ["views_count", "like_count", "comment_count"]:
                data["data"][k] = int(data["data"].get(k, 0))
            df = spark.createDataFrame([data["data"]], schema=video_schema)
            df.write.mode("append").parquet(HDFS_VIDEO_PATH)
            print(f" Stored video: {data['data']['title']}")
        except Exception as e:
            print(f"[ERROR] Video store failed: {e}\nData: {data}")

def process_and_store():
    """
    Starts the Kafka consumer loop and processes messages as they arrive.
    Uses Spark to convert and store messages into HDFS.
    Acts as the persistent streaming ingestion engine.
    """
    for message in consumer:
        print(f"\nReceived:\n{message.value}")
        process_message(message)

if __name__ == "__main__":
    print("Kafka consumer started...")
    process_and_store()

