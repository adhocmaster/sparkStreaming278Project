# Spark Streaming 278 Project
# PROJECT: ANALYZING BATCH AND PARTITION CONFIGURATION ON STREAM PROCESSING IN SPARK.
The goal of the project is two-fold: finding optimal batching and partitioning for a specific dataset and application.
Golam Md Muktadir


## 1. DATA
| Type        | Description  |
| ------------- | ----- |
| Dataset     | USA Name Data (BigQuery Dataset)
| License     | Open for analysis
| Owner | Data.gov
| History | This public dataset was created by the Social Security Administration and contains all names from Social Security card applications for births that occurred in the United States after 1879.


## 2. SOME EXAMPLES OF QUERIES:
1. What are the most common names?
2. What are the most common female names?
3. Are there more female or male names?
4. Female names by a wide margin?
5. Impact of Game of Thrones on US Baby Names! (https://www.kaggle.com/dhanushkishore/impact-of-game-of-thrones-on-us-baby-names)
6. Impact of TV series on US Baby Names! (Not yet done) OUTCOME

## 3. Objectives
For each operation an impact of batching and partition on request time and throughput. Targeting to learn spark streaming 2.0.
1. One query, and throughput.
2. One query, throughput, and request time.