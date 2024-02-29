import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def filter_and_rename_data(df_clients, df_financial, countries):
    clients_filt = df_clients.filter(col("country").isin(countries))
    clients_filt = clients_filt.drop("first_name", "last_name")
    df_financial = df_financial.drop("cc_n")
    clients_rename = clients_filt.withColumnRenamed("id", "client_identifier").withColumnRenamed("btc_a", "bitcoin_address")
    financial_rename = df_financial.withColumnRenamed("id", "client_identifier").withColumnRenamed("cc_t", "credit_card_type")
    joined_data = clients_rename.join(financial_rename, "client_identifier", "inner")

    return joined_data

def save_output(data, output_path):
    data.write.option("header", "true").csv(output_path)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("app.log"),
            logging.StreamHandler()
        ]
    )

def main(clients_file, financial_file, output_path, countries):

    spark = SparkSession.builder.appName("KommatiPara Data Processing").getOrCreate()
    spark.sparkContext.setLogLevel("info")

    try:
        df_clients = spark.read.option("header", "true").csv(clients_file)
        df_financial = spark.read.option("header", "true").csv(financial_file)
        processed_data = filter_and_rename_data(df_clients, df_financial, countries)
        save_output(processed_data, output_path)

        logging.info("Data processing completed")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python script.py <clients_file> <financial_file> <output_path> <country1,country2>")
        sys.exit(1)

    clients_file = sys.argv[1]
    financial_file = sys.argv[2]
    output_path = os.path.join("client_data", sys.argv[3])
    countries = sys.argv[4].split(",")

    setup_logging()

    main(clients_file, financial_file, output_path, countries)
  