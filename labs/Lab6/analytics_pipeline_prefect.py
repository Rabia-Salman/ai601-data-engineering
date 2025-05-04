from prefect import task, flow, get_run_logger
import pandas as pd
import matplotlib.pyplot as plt

data_path = "data/analytics_data.csv"
summary_path = "data/analytics_summary.csv"
histogram_path = "data/sales_histogram.png"

@task
def fetch_data():
    logger = get_run_logger()
    logger.info("Reading data from CSV...")
    df = pd.read_csv(data_path)
    logger.info(f"Data shape: {df.shape}")
    return df

@task
def validate_data(df: pd.DataFrame):
    logger = get_run_logger()
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    df_clean = df.dropna()
    return df_clean

@task
def transform_data(df: pd.DataFrame):
    logger = get_run_logger()
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
        logger.info("Sales data normalized.")
    return df

@task
def generate_report(df: pd.DataFrame):
    logger = get_run_logger()
    summary = df.describe()
    summary.to_csv(summary_path)
    logger.info(f"Summary statistics saved to {summary_path}")

@task
def create_histogram(df: pd.DataFrame):
    logger = get_run_logger()
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig(histogram_path)
        plt.close()
        logger.info(f"Sales histogram saved to {histogram_path}")

@flow
def analytics_pipeline():
    df = fetch_data()
    df_clean = validate_data(df)
    df_transformed = transform_data(df_clean)
    generate_report(df_transformed)
    create_histogram(df_transformed)
    
if __name__ == "__main__":
    analytics_pipeline()
