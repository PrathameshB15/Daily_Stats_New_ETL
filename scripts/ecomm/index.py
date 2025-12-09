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

    # Power BI login details
    LOGIN_URL = 'https://bi-api.beastinsights.co/api/user/login'
    USERNAME = "jwfncj"
    PASSWORD = "vwjnvj"
    DATA_URL_TEMPLATE = "https://bi-api.beastinsights.co/api/page/executeQueries/{}"

    #login api to get authentication cookie
    def login_and_extract_cookies(username: str, password: str):
        """
        Logs in to the API and extracts cookies from the response.

        Args:
            username (str): The username or email for login.
            password (str): The password for login.

        Returns:
            dict: A dictionary containing the extracted cookies.
        """
        url = 'https://bi-api.beastinsights.co/api/user/login'
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,fr;q=0.8',
            'content-type': 'application/json',
            'origin': 'https://app.beastinsights.co',
            'referer': 'https://app.beastinsights.co/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        payload = {
            "usernameOrEmail": username,
            "password": password
        }
        
        # Make the POST request
        response = requests.post(url, headers=headers, json=payload)
        
        # Check if the response was successful
        if response.status_code == 200:
            return response.cookies.get_dict()
        else:
            raise Exception(f"Failed to login: {response.status_code}, {response.text}")

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

    def process_campaign_data(url, headers, payload, client_id):
        max_retries = 5
        retry_delay = 5
        attempt = 0

        while attempt < max_retries:
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=300)
                response.raise_for_status()

                if response.status_code == 200:
                    print("Request successful:", response.json())
                    break
                else:
                    print(f"Attempt {attempt + 1}: Received status code {response.status_code}. Retrying...")

            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1}: Error - {e}. Retrying...")

            attempt += 1
            time.sleep(retry_delay)

        if attempt == max_retries:
            print("Max retries reached. Request failed.")
            return None

        data = response.json()

        # Extract rows from the JSON structure
        rows = data['data']['results'][0]['tables'][0]['rows']
        df = pd.DataFrame(rows)

        if df.empty:
            return None

        # 1) Rename to simple column names
        df = df.rename(columns={
            'payments[lender]': 'Acquirer',
            'payments[corp]'  : 'Corp',
            '[Net revenue]'   : 'NetRevenue'
        })

        # 2) Make NetRevenue numeric (handles $, commas, blanks, etc.)
        df['NetRevenue'] = (
            df['NetRevenue']
            .astype(str)
            .str.replace(r'[\$,]', '', regex=True)   # drop $ and commas if present
            .str.replace(r'\((.*)\)', r'-\1', regex=True)  # (123.45) -> -123.45
            .str.strip()
            .replace({'': None})
            .pipe(pd.to_numeric, errors='coerce')
        )

        # 3) Keep Lender/Corp as text (optionally standardize whitespace/case)
        df['Acquirer'] = df['Acquirer'].astype(str).str.strip()
        df['Corp']   = df['Corp'].astype(str).str.strip()

        


        # Extract to dictionary
        campaign_data = {
            "Metric": [],
            "Value": []
        }

        # for i in range(min(5, len(df))):
        for i in range(len(df)):
            campaign = df.iloc[i]
            acquirer = str(campaign['Acquirer'])
            corp = str(campaign['Corp'])
            net_revenue = str(round(campaign['NetRevenue']))

            campaign_data["Metric"].extend([
                f"Campaign {i+1} Acquirer", 
                f"Campaign {i+1} Corp", 
                f"Campaign {i+1} NetRevenue"
            ])
            campaign_data["Value"].extend([
                acquirer,
                corp,
                net_revenue
            ])

        # campaign_data["Metric"].extend(["client_id", "last_daily_update_xpay"])
        # campaign_data["Value"].extend([str(client_id), str(int(datetime.now().timestamp()))])
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

        try:
            subprocess.run([
                "node", puppeteer_script, html_path, output_path
            ], check=True)
            print(f"PNG image saved at: {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to generate PNG for {html_path}: {e}")


    def generate_dashboard_jinja(campaign_data_1,  client_id):
        # If both datasets are empty or missing required keys, skip generation
        if not (campaign_data_1 and campaign_data_1.get("Metric") and campaign_data_1.get("Value")):
            print("No campaign data to display. Skipping HTML generation.")
            return None  # or raise an exception, or handle as needed
        
        with open('scripts/ecomm/html/campaign-dashboard-template.html', 'r') as file:
            template = Template(file.read())
        
        html_content = template.render(
            campaign_data_1=json.dumps(campaign_data_1 or {})
        )
        
        output_folder = "scripts/ecomm/html/output"  # Define the folder path
        os.makedirs(output_folder, exist_ok=True) 

        output_filename = os.path.join(output_folder, f"{client_id}.html") 

        
        with open(output_filename, 'w') as file:
            file.write(html_content)
        
        return output_filename
    # Initialize an empty dictionary to store results by client
    client_results = {}

    # Initialize an empty list to store the final result
    result = []

    client_ids = [10049]
    # client_ids = [10011, 10012, 10013, 10014, 10016]
    # client_ids = [10009]
    
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
                        # Construct the API URL for each page_id
                        cookies = login_and_extract_cookies(f"dev_{client_id}", "Dev@beastinsights/#2024")
                        
                        url = f"https://bi-api.beastinsights.co/api/page/executeQueries/{page_id}"

                        # Define the headers and payload for the API request
                        headers = {
                            'Content-Type': 'application/json',
                            'cookie': '; '.join([f'{key}={value}' for key, value in cookies.items()])
                        }

                        # Define the payload with Power BI DAX or M queries
                        
                        payload = {
                            "query": """
                            EVALUATE
                            SUMMARIZECOLUMNS(payments[lender],payments[corp],
                            FILTER('Calendar','Calendar'[Date]=TODAY()-1),"Net revenue",[Net Revenue]) 
                            ORDER by [Net revenue] DESC
                            """
                        }

                        try:
                            
                            campaign_data_1 = process_campaign_data(url, headers, payload, client_id)
                            # campaign_data_2 = process_campaign_data(url, headers, payload_2, client_id)

                            # Calculate yesterday's date
                            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                            # Add yesterday's date to the dictionary
                            # Append to the lists
                            # campaign_data_1['Metric'].append('Date')
                            # campaign_data_1['Value'].append(yesterday)
                            
                            # Generate the dashboard
                            output_file = generate_dashboard_jinja(campaign_data_1, client_id)
                            if output_file:
                                print(f"Dashboard generated: {output_file}")
                            else:
                                break

                            image_folder = "images"
                            os.makedirs(image_folder, exist_ok=True)
                            
                            png_path = os.path.join(image_folder, f"custom_{client_id}.png")
                            
                            html_to_png(output_file, png_path)

                            
                            print(f"PNG for {client_id} saved at: {png_path}")
                            
                            
                            metrics = {
                                "client_id": str(client_id),
                                "last_daily_update_custom": str(int(datetime.now().timestamp()))
                            }

                            push_to_customer_io(client_id, metrics)
                            
                            # Configure Cloudinary (replace with your credentials)
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

    html_directory = 'scripts/xpay/html/output'
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

    # Code to send email
    send_email("Daily metrics refresh Customer.io", final_message, attachment_path=log_file)
    print("Email sent")

    # Delete the log file after sending the email
    if os.path.exists(log_file):
        os.remove(log_file)
    
else:
    print(f"⏭️ Skipping {script_name}")
