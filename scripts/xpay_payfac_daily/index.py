import psycopg2
import pandas as pd
import configparser
import requests
import configparser
import psycopg2
import pandas as pd
import requests
import subprocess
from decimal import Decimal
import os
from PIL import Image, ImageDraw, ImageFont
import io
from textwrap import wrap
#import boto3
#from botocore.exceptions import NoCredentialsError
from datetime import datetime, timedelta
#from pydrive2.auth import GoogleAuth
#from pydrive2.drive import GoogleDrive
import cloudinary
import cloudinary.uploader
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from execution_summary import summary
from email_notification import send_email
import time
from jinja2 import Environment, FileSystemLoader, Template
import json
import re
import argparse


def parse_arguments():
    """Parse command line arguments for client IDs."""
    parser = argparse.ArgumentParser(description='Process daily stats for specific client IDs')
    parser.add_argument('--client-ids', type=str, 
                       help='Comma-separated list of client IDs to process')
    args = parser.parse_args()
    
    client_ids = []
    if args.client_ids:
        client_ids = [int(id.strip()) for id in args.client_ids.split(',') if id.strip()]
    
    return client_ids




# Load the main config.ini
config = configparser.ConfigParser()
config.read("config.ini")

script_name = os.path.basename(os.path.dirname(__file__))

# Check if the script should run
if config['General'].getboolean(f'run_{script_name}'):
    print(f"✅ Running {script_name}...")
    # Your existing script logic here
    
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Customer.io API settings
    CUSTOMER_IO_API_KEY = config.get("customer_io", "API_KEY")
    site_id = config.get("customer_io", "site_id")
    CUSTOMER_IO_BASE_URL = 'https://track.customer.io/api/v1/customers/'

    # Database connection details
    PSG_USER = config.get("production", "PSG_USER")
    PSG_PASSWORD = config.get("production", "PSG_PASSWORD")
    PSG_HOST = config.get("production", "PSG_HOST")
    PSG_PORT = int(config.get("production", "PSG_PORT"))
    PSG_DATABASE = config.get("production", "PSG_DATABASE")



    # Function to fetch data from PostgreSQL
    def fetch_data_from_db(query):
        try:
            # Establish connection to PostgreSQL database
            conn = psycopg2.connect(
                user=PSG_USER,
                password=PSG_PASSWORD,
                host=PSG_HOST,
                port=PSG_PORT,
                database=PSG_DATABASE,
            )

            # Create a cursor object
            cur = conn.cursor()

            # Execute the query
            cur.execute(query)

            # Fetch the result and create DataFrame
            result = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(result, columns=colnames)

            # Close the cursor and connection
            cur.close()
            conn.close()

            return df

        except Exception as e:
            print(f"An error occurred while fetching data: {e}")
            return None

    # Convert values to float before addition (handle potential None values)
    def to_float(value):
        try:
            return float(value) if value is not None else 0
        except ValueError:
            return 0
        
    def merge_campaign_data(data1, label1, data2, label2):
        merged = {
            "Metric": [],
            "Value": []
        }

        for metric, value in zip(data1["Metric"], data1["Value"]):
            merged["Metric"].append(f"{label1} - {metric}")
            merged["Value"].append(value)

        for metric, value in zip(data2["Metric"], data2["Value"]):
            merged["Metric"].append(f"{label2} - {metric}")
            merged["Value"].append(value)

        return merged

    # Converts decimal numbers to float
    def convert_decimal_to_float(data):
        if isinstance(data, dict):
            return {key: convert_decimal_to_float(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [convert_decimal_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        elif data is None:
            return 0  # Replace None with 0
        else:
            return data

    # Push metrics to customer.io
    def push_to_customer_io(db_id, metrics):
        
        # Convert any Decimal values to float
        metrics = convert_decimal_to_float(metrics)
        
        url = f"{CUSTOMER_IO_BASE_URL}{db_id}"
        
        url = f"https://track.customer.io/api/v1/customers/{db_id}"
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.put(url, headers=headers, json=metrics, auth=(site_id, CUSTOMER_IO_API_KEY))
        if response.status_code == 200:
            print(f"Metrics successfully pushed for customer {db_id}")
            summary.add_success(f"Daily Metrics have been refreshed for client {db_id}.")

        else:
            print(f"Failed to push metrics for customer {db_id}: {response.status_code} {response.text}")
            summary.add_failure(
                f"Daily Metrics refresh failed for client {db_id}", 
                f"Daily Metrics refresh failed for client {db_id} : {str(e)}"
            )

    # Upload images over cloud platform
    def upload_images_to_cloudinary():
        # Folder to look for generated images
        images_folder = 'images'
        
        # Iterate over all files in the images folder
        for image_name in os.listdir(images_folder):
            if image_name.endswith(".png"):  # Only process PNG images
                image_path = os.path.join(images_folder, image_name)
                
                # Set the public_id with the folder and file name
                public_id = f"daily_images/{os.path.splitext(image_name)[0]}"  # Folder is 'daily_images'

                try:
                    # Upload the image to Cloudinary, overwriting if it already exists
                    response = cloudinary.uploader.upload(
                        image_path,
                        public_id=public_id,  # Use folder name + image name as public_id
                        overwrite=True,         # Enable overwrite
                        invalidate=True       # Force cache invalidation
                    )
                    
                    print(f"Uploaded '{image_name}' to Cloudinary.")
                    print(f"Image URL: {response['secure_url']}")
                
                except Exception as e:
                    print(f"Error uploading {image_name}: {str(e)}")

    def process_sql_campaign_data(df, client_id):
        """Process SQL query results for payfac product data"""
        if df.empty:
            return None

        # Ensure numeric columns are properly typed
        df = df.fillna(0).replace('', 0)
        df['attempts'] = pd.to_numeric(df['attempts'], errors='coerce').fillna(0).astype(int)
        df['approvals'] = pd.to_numeric(df['approvals'], errors='coerce').fillna(0).astype(int)
        df['approval_rate'] = pd.to_numeric(df['approval_rate'], errors='coerce').fillna(0)
        df['cb_count'] = pd.to_numeric(df['cb_count'], errors='coerce').fillna(0).astype(int)
        df['cb_rate'] = pd.to_numeric(df['cb_rate'], errors='coerce').fillna(0)

        # Sort by Approvals in descending order (high to low)
        df = df.sort_values('approvals', ascending=False)

        # Extract to dictionary
        campaign_data = {
            "Metric": [],
            "Value": []
        }

        # Process each product with the complete 6-column structure
        for i in range(len(df)):
            product = df.iloc[i]
            product_name = str(product['product']) if pd.notna(product['product']) else "N/A"
            attempts = f"{int(product['attempts']):,}" if pd.notna(product['attempts']) else "0"
            approvals = f"{int(product['approvals']):,}" if pd.notna(product['approvals']) else "0"
            
            # Approval rate is already in percentage format from SQL query
            approval_rate = f"{product['approval_rate']:.1f}%" if pd.notna(product['approval_rate']) else "0.0%"
            
            cb_count = f"{int(product['cb_count']):,}" if pd.notna(product['cb_count']) else "0"
            
            # CB rate is already in percentage format from SQL query
            cb_rate = f"{product['cb_rate']:.1f}%" if pd.notna(product['cb_rate']) else "0.0%"

            campaign_data["Metric"].extend([
                f"Product {i+1} Name", 
                f"Product {i+1} Attempts",
                f"Product {i+1} Approvals", 
                f"Product {i+1} Approval Rate",
                f"Product {i+1} CB Count",
                f"Product {i+1} CB Rate"
            ])
            campaign_data["Value"].extend([
                product_name,
                attempts,
                approvals,
                approval_rate,
                cb_count,
                cb_rate
            ])

        # Calculate yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        # Add yesterday's date to the dictionary
        campaign_data['Date'] = yesterday
        campaign_data['client_id'] = client_id

        return campaign_data
    

    def html_to_png(html_path, output_path):
        puppeteer_script = os.path.join("puppeteer-screenshot", "screenshot.js")

        # Ensure .png extension
        if not output_path.endswith(".png"):
            output_path = os.path.splitext(output_path)[0] + ".png"

        # Use absolute paths so Node has no confusion about CWD
        html_abs = os.path.abspath(html_path)
        out_abs = os.path.abspath(output_path)
        script_abs = os.path.abspath(puppeteer_script)

        # Optional: check if the HTML file actually exists
        if not os.path.exists(html_abs):
            print(f"❌ HTML file does not exist: {html_abs}")
            return

        print("Running Puppeteer with:")
        print(f"  script : {script_abs}")
        print(f"  html   : {html_abs}")
        print(f"  output : {out_abs}")

        try:
            result = subprocess.run(
                ["node", script_abs, html_abs, out_abs],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"✅ PNG image saved at: {out_abs}")
            if result.stdout:
                print("Puppeteer stdout:\n", result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to generate PNG for {html_abs}")
            print(f"Return code: {e.returncode}")
            if e.stdout:
                print("Puppeteer stdout:\n", e.stdout)
            if e.stderr:
                print("Puppeteer stderr:\n", e.stderr)



    def generate_dashboard_jinja(campaign_data_1, client_id):
        # If dataset is empty or missing required keys, skip generation
        if not (campaign_data_1 and campaign_data_1.get("Metric") and campaign_data_1.get("Value")):
            print("No campaign data to display. Skipping HTML generation.")
            return None  # or raise an exception, or handle as needed
        
        with open('scripts/xpay_payfac_daily/html/campaign-dashboard-template.html', 'r') as file:
            template = Template(file.read())
        
        html_content = template.render(
            campaign_data_1=json.dumps(campaign_data_1 or {})
        )
        
        output_folder = "scripts/xpay_payfac_daily/html/output"  # Define the folder path
        os.makedirs(output_folder, exist_ok=True) 

        output_filename = os.path.join(output_folder, f"{client_id}.html") 

        
        with open(output_filename, 'w') as file:
            file.write(html_content)
        
        return output_filename
    # Initialize an empty dictionary to store results by client
    client_results = {}

    # Initialize an empty list to store the final result
    result = []

    # Parse command line arguments for client IDs
    client_ids = parse_arguments()
    
    # If no client IDs provided via command line, use default client IDs
    if not client_ids:
        client_ids = [10010, 10016]
        print("No client IDs provided. Using default client IDs:", client_ids)
    else:
        print("Processing client IDs:", client_ids)
    
    # Loop over each client_id to fetch the respective report_id and product_id
    for client_id in client_ids:
        # Fetch report ids from the "reports" table
        report_query = f"select id from beast_insights_v2.reports where client_id = {client_id} and is_active = 'True' and is_deleted = 'False' limit 1;"
        reports_df = fetch_data_from_db(report_query)

        if reports_df is not None:
            for report_id in reports_df['id']:
                # Fetch product ids from the "products" table
                page_query = f"SELECT id FROM beast_insights_v2.pages WHERE report_id = {report_id} and is_active = 'True' and is_deleted = 'False' limit 1;"
                page_df = fetch_data_from_db(page_query)

                if page_df is not None:
                    for page_id in page_df['id']:
                        try:
                            # Execute SQL query to get payfac product metrics directly from database
                            payfac_daily_query = f"""
                            WITH payfac_gateways AS (
                                SELECT DISTINCT gateway_id
                                FROM payments
                                WHERE lender ILIKE '%payfac%'
                                  AND client_id = {client_id}
                            )
                            SELECT
                                p.product_name AS product,
                                SUM(os.attempts_total) AS attempts,
                                SUM(os.approvals) AS approvals,
                                SUM(os.approvals)::decimal
                                    / NULLIF(SUM(os.attempts_total), 0) * 100 AS approval_rate,
                                SUM(os.cb_cb_date) AS cb_count,
                                SUM(os.cb_cb_date)::decimal
                                    / NULLIF(SUM(os.attempts_total), 0) * 100 AS cb_rate
                            FROM reporting.order_summary_{client_id} os
                            JOIN payfac_gateways pg
                                ON pg.gateway_id = os.gateway_id
                            LEFT JOIN products p
                                ON p.product_id = os.product_id
                               AND p.client_id = {client_id}
                            WHERE os.date = CURRENT_DATE - 1
                            GROUP BY p.product_name
                            ORDER BY p.product_name;
                            """
                            
                            # Execute the query
                            df = fetch_data_from_db(payfac_daily_query)
                            
                            if df is not None and not df.empty:
                                print(f"Payfac daily product metrics for client {client_id}:")
                                print(df)
                                
                                # Process the SQL query results directly
                                campaign_data_1 = process_sql_campaign_data(df, client_id)
                            else:
                                print(f"No payfac daily product data found for client {client_id}")
                                campaign_data_1 = None

                            if campaign_data_1 is None:
                                continue
                            
                            # Generate the dashboard
                            output_file = generate_dashboard_jinja(campaign_data_1, client_id)
                            if output_file:
                                print(f"Dashboard generated: {output_file}")
                            else:
                                break

                            image_folder = "images"
                            os.makedirs(image_folder, exist_ok=True)
                            
                            png_path = os.path.join(image_folder, f"xpay_{client_id}_payfac.png")
                            
                            html_to_png(output_file, png_path)

                            
                            print(f"PNG for {client_id} saved at: {png_path}")
                            
                            
                            metrics = {
                                "client_id": str(client_id),
                                "last_daily_update_xpay_payfac": str(int(datetime.now().timestamp()))
                            }

                            push_to_customer_io(client_id, metrics)
                            
                        #     # Configure Cloudinary (replace with your credentials)
                            cloudinary.config(
                                cloud_name=config.get("production", "cloud_name"),  # Replace with your Cloudinary cloud name
                                api_key=config.get("production", "api_key"),        # Replace with your Cloudinary API key
                                api_secret=config.get("production", "api_secret")   # Replace with your Cloudinary API secret
                            )
                            
                        #     # Call the function to upload images
                            upload_images_to_cloudinary()

                        #     # upload_images_to_drive(google_drive_folder_id)
                        #     # Directory path to the 'images' folder
                        #     images_directory = 'images'

                        #     # Check if the directory exists
                        #     if os.path.exists(images_directory):
                        #         # Iterate over all files in the directory and remove them
                        #         for file_name in os.listdir(images_directory):
                        #             file_path = os.path.join(images_directory, file_name)
                        #             # Only delete files (not subdirectories)
                        #             if os.path.isfile(file_path):
                        #                 os.remove(file_path)
                        #                 print(f"Deleted {file_name}")
                        #     else:
                        #         print(f"Directory {images_directory} does not exist.")
                            
                        #     print("Execution completed successfully, removed files from images directory")

                        except requests.exceptions.RequestException as e:
                            print(f"API request failed for page_id {page_id}: {e}")
                            summary.add_failure(
                                f"Daily Metrics refresh failed for client {client_id}", 
                                f"Daily Metrics refresh failed for client {client_id} : {str(e)}"
                            )
    

    # Configure Cloudinary (replace with your credentials)
    cloudinary.config(
        cloud_name=config.get("production", "cloud_name"),  # Replace with your Cloudinary cloud name
        api_key=config.get("production", "api_key"),        # Replace with your Cloudinary API key
        api_secret=config.get("production", "api_secret")   # Replace with your Cloudinary API secret
    )
    
    # Call the function to upload images
    # upload_images_to_cloudinary()

    images_directory = 'images'

    # Check if the directory exists
    if os.path.exists(images_directory):
        # Iterate over all files in the directory and remove them
        for file_name in os.listdir(images_directory):
            if file_name.lower().endswith(".png"):
                file_path = os.path.join(images_directory, file_name)
                # Only delete files (not subdirectories)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Deleted {file_path}")
    else:
        print(f"Directory {images_directory} does not exist.")
    
    print("Execution completed successfully, removed files from images directory")

    html_directory = 'scripts/xpay_payfac_daily/html/output'
    # Check if the HTML directory exists and delete .html files
    if os.path.exists(html_directory):
        for root, _, files in os.walk(html_directory):
            for file_name in files:
                if file_name.lower().endswith(".html"):
                    file_path = os.path.join(root, file_name)
                    os.remove(file_path)
                    print(f"Deleted {file_path}")
    else:
        print(f"Directory {html_directory} does not exist.")

            # After all functions are executed, send the email
    final_message = summary.get_summary()

    # Save logs to a .txt file
    log_file = 'logs/execution_logs.txt'
    summary.save_logs_to_file(log_file)
    print("log file created")

    # Code to send email (commented out to avoid authentication issues)
    # send_email("Daily metrics refresh Customer.io", final_message, attachment_path=log_file)
    print("Email sending skipped")

    # Delete the log file after sending the email
    if os.path.exists(log_file):
        os.remove(log_file)
    
else:
    print(f"⏭️ Skipping {script_name}")
