# PySparkTransformations

This project demonstrates how to perform end-to-end data engineering using PySpark on the MovieLens dataset, including cleaning, normalization, broadcasting, and analytical aggregation.

---

##  Goal 1: Load and Validate Movie Data (Bronze Layer)

- Ingest the raw `movies.csv` using a custom-defined Spark `StructType` schema  
- Validate schema consistency and data types upon ingestion  
- Handle missing or malformed records gracefully

---

##  Goal 2: Extract & Normalize Metadata

- Extract movie release `year` from the title using regular expressions  
- Clean and trim extra characters from the `title` column  
- Split the `genres` string into arrays and **explode** them into separate rows  
- **Pivot** genres to binary flags ('yes'/'no') for easier filtering and modeling  
- Drop noisy or malformed columns and fill nulls with default values  
- Save the cleaned movie dataset to the **Silver Layer** (`movies_transformed.csv`)

---

##  Goal 3: Load and Enrich Ratings Data

- Ingest `ratings.csv` using an explicitly defined schema with proper data types  
- Convert `timestamp` from UNIX to human-readable format using `from_unixtime()`  
- Extract only the date component using `date_format()` for daily-level analysis  
- Save the refined ratings data to the **Silver Layer** (`ratings_transformed.csv`)

---

##  Goal 4: Use of Broadcast Variables for Performance Optimization

- Convert the final movie DataFrame to a dictionary mapping `movieId` → `movie_name`  
- Broadcast this dictionary using Spark’s `broadcast()` for optimized joins  
- Enrich the ratings RDD with movie names using this broadcast variable  
- Create a new Spark DataFrame (`df_final_ratings_withnames`) containing both rating and movie name

---

##  Goal 5: Aggregation & Insight Generation

- Group enriched ratings by `movieId` and `movie_name` to compute average ratings  
- Sort and display top-rated or most-reviewed movies  
- Provide a clean foundation for further BI reporting or ML model training

---

### 💾 Output Location

All processed files are saved into the **Silver Layer**:

- `/content/drive/MyDrive/Silver_Layer/movies_transformed.csv`  
- `/content/drive/MyDrive/Silver_Layer/ratings_transformed.csv`

---

### 💡 Technologies Used

- Apache Spark (PySpark)  
- Google Colab (for execution)  
- Google Drive (as the data lake)  
- Regular Expressions, Unix Timestamp Conversion, Pivoting, Broadcasting
