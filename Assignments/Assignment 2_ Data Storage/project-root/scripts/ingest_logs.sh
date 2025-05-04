#!/bin/bash
# File: project-root/scripts/ingest_logs.sh
# Usage: ./ingest_logs.sh <YYYY-MM-DD>
# This script ingests user activity log CSV files into HDFS.

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <YYYY-MM-DD>"
    exit 1
fi

DATE="$1"
YEAR=$(echo "$DATE" | cut -d'-' -f1)
MONTH=$(echo "$DATE" | cut -d'-' -f2)
DAY=$(echo "$DATE" | cut -d'-' -f3)

LOCAL_LOG_DIR="C:/Users/aizel/Downloads/LUMS OFFICIAL DATA/Data Eng/labs/data-ingestion-hive/project-root/user_activity_logs"
LOCAL_META_DIR="C:/Users/aizel/Downloads/LUMS OFFICIAL DATA/Data Eng/labs/data-ingestion-hive/project-root/raw_data/metadata"

HDFS_LOG_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_META_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

hdfs dfs -mkdir -p "$HDFS_LOG_DIR"
hdfs dfs -mkdir -p "$HDFS_META_DIR"

LOG_FILE="$LOCAL_LOG_DIR/user_activity_logs_${DATE}.csv"
if [ -s "$LOG_FILE" ]; then
    hdfs dfs -put "$LOG_FILE" "$HDFS_LOG_DIR/"
    echo "Ingested logs from $LOG_FILE into $HDFS_LOG_DIR"
else
    echo "Warning: Log file $LOG_FILE is empty or does not exist."
fi

META_FILE="C:/Users/aizel/Downloads/LUMS OFFICIAL DATA/Data Eng/labs/data-ingestion-hive/project-root/content_metadata.csv"
if [ -s "$META_FILE" ]; then
    hdfs dfs -put "$META_FILE" "$HDFS_META_DIR/"
    echo "Ingested metadata from $META_FILE into $HDFS_META_DIR"
else
    echo "Warning: Metadata file $META_FILE is empty or does not exist."
fi

echo "Ingestion process complete."
