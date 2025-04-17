📘 Project Overview

The YouTube Streaming ETL Pipeline is a real-time, production-ready data engineering project designed to continuously extract, transform, and load YouTube video and channel data using modern big data tools.

This Pipeline follows best practices in stream processing, fault tolerance, schema validation, and distributed analytics.


🛠️ Core Technologies
Tools | Role

🧠 YouTube Data API | Data Source (Videos + Channels)
🛰️ Apache Kafka | Real-time Streaming Layer
🔥 Apache Spark PySpark | Stream Consumer + Transformation
🗃️ Apache Hadoop HDFS (Parquet) | Scalable Data Lake Storage
🧩 Anaconda Jupyter Notebook | Data Analysis and finally create json file to store seen video id to prevent duplicates while excuting 
 

🚀 Key Features

✅ Initial Load: Fetches up to 50 videos per channel
🔁 Streaming Poll: Extracts new uploads every 10 minutes
📡 Kafka Integration: Seamless producer/consumer model
🧪 Spark Processing: Schema validation & transformation
📁 HDFS Parquet Output: Analytics-ready, columnar storage
🚫 Duplicate Prevention: Via seen_video_ids.json
📊 Data Analytics Highlights

After successfully ingesting data into HDFS using the streaming ETL pipeline, we conducted exploratory data analysis using PySpark, Pandas, and Matplotlib in Jupyter Notebook (data_analysis.ipynb). The goal was to generate meaningful insights from real YouTube content and enhance visibility into media trends.
🔍 Key Analysis Techniques:

  📌 Top Video Analysis: Ranking videos by view count, likes, and comment activity.

    
  💬 Comment Insights:

   * Fetched top-level comments per video

   * Extracted common keywords using text preprocessing

  ☁️ Word Cloud Generation:
   
   * Built word clouds from video titles, tags, and comments

   * Highlighted trending keywords and viewer themes

  📈 Visualizations:

   * Bar charts, pie charts, and line plots to explore:

     * Most active channels

     * Engagement over time

     * Content format popularity

These analyses not only showcase the depth of data collected but also help inform strategies for content optimization, user engagement, and machine learning integration.




🔗 Data Flow (Simplified Architecture)

YouTube API -> Kafka Producer (Python) -> Kafka Topic -> Kafka Consumer (PySpark) -> HDFS (Parquet)




📊 Real-World Use Cases

  *  Track newly uploaded videos in near real-time

  *  Analyze comment engagement and sentiment

  *  Visualize accessibility insights (e.g., captioned content)

  *  Feed ML models with video metadata (quality, tags, etc.)











