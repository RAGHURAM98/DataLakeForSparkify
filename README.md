# Creating a Data Lake on AWS using Apache Spark

## Table of Contents
1. [Project Summary](#Project-Sumary)
2. [Installation](#Installation)
3. [File Descriptions](#File-Descriptions)

## Project Summary <a name="Project-Sumary"></a>
1. Data should be extracted from 's3a://udacity-dend/''
2. Data should be transformed and inserted into analytics tables using Spark
3. The analyitcs tables should be loaded back into another S3 Bucket as parquet.

## Installation <a name="Installation"></a>
1. Add your AWS keys amd output_folder to dl.cfg 
2. Make sure all libraries imported in etl.py are installed in your Python environment
3. Run the 'python etl.py' or 'spark-submit etl.py' in order to trigger the etl process

## File Descriptions <a name="File-Descriptions"></a>
1. dl.cfg: Template for the aws configration and output folder name
2. etl.py: Implements the ETL process


