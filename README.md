# aviabnapp

KommatiPara 
Overview
This PySpark application is designed for processing client data for KommatiPara, a company specializing in bitcoin trading. The application reads two separate datasets containing information about clients and their financial details, filters clients from specified countries (United Kingdom and Netherlands in this case), removes personal identifiable information and sensitive financial data, renames columns for better readability, and finally saves the processed data to a specified directory.

Usage
To use the application, follow these steps:

Ensure you have Python 3.8 installed on your system.
Install Apache Spark and its Python library (pyspark).
Clone the project repository from GitHub.
Prepare your datasets:
Dataset One: Contains client information including id, first_name, last_name, email, and country.
Dataset Two: Contains financial details including id, btc_a, cc_t, and cc_n.
Run the application using the spark-submit command with the following arguments:
<clients_file>: Path to the dataset containing client information.
<financial_file>: Path to the dataset containing financial details.
<output_dir>: Directory where the processed data will be saved.
<countries>: Comma-separated list of countries to filter (e.g., "Netherlands,United Kingdom").
Example command:

bash
Copy code
spark-submit script.py dataset_one.csv dataset_two.csv client_data Netherlands,United Kingdom
Output
The application generates a CSV file containing the processed client data, which is saved in the specified output directory (client_data in this case). The output file contains columns such as client_identifier, bitcoin_address, and credit_card_type, with personal identifiable information and sensitive financial data removed for privacy and security purposes.
