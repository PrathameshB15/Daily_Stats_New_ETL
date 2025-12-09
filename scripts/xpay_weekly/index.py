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
import base64
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
import argparse


def parse_arguments():
    """Parse command line arguments for client IDs."""
    parser = argparse.ArgumentParser(description='Process weekly stats for specific client IDs')
    parser.add_argument('--client-ids', type=str, 
                       help='Comma-separated list of client IDs to process')
    args = parser.parse_args()
    
    client_ids = []
    if args.client_ids:
        client_ids = [int(id.strip()) for id in args.client_ids.split(',') if id.strip()]
    
    return client_ids
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

    def process_retention_data(url, headers, payload, client_id, step_name):
        max_retries = 3  # Reduced retries for faster execution
        retry_delay = 3  # Reduced delay
        attempt = 0

        print(f"Starting {step_name} query execution...")
        
        while attempt < max_retries:
            try:
                # Increased timeout for large queries
                response = requests.post(url, headers=headers, json=payload, timeout=600)
                response.raise_for_status()

                if response.status_code == 200:
                    print(f"✅ {step_name} query completed successfully")
                    break
                else:
                    print(f"❌ Attempt {attempt + 1}: Received status code {response.status_code} for {step_name}. Retrying...")

            except requests.exceptions.Timeout:
                print(f"⏰ Attempt {attempt + 1}: Timeout occurred for {step_name}. Retrying...")
            except requests.exceptions.RequestException as e:
                print(f"❌ Attempt {attempt + 1}: Error for {step_name} - {e}. Retrying...")

            attempt += 1
            if attempt < max_retries:
                print(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)

        if attempt == max_retries:
            print(f"❌ Max retries reached for {step_name}. Skipping this step.")
            return None

        try:
            data = response.json()
            print(f"📊 Processing response data for {step_name}...")

            # Extract rows from the JSON structure
            rows = data['data']['results'][0]['tables'][0]['rows']
            df = pd.DataFrame(rows)

            if df.empty:
                print(f"⚠️  No data returned for {step_name}")
                return None

            # Debug: Print original column names before cleaning
            print(f"🔍 Original column names for {step_name}: {df.columns.tolist()}")
            
            # Clean column names - remove brackets and quotes properly
            cleaned_columns = []
            for col in df.columns:
                # Remove all brackets and quotes from anywhere in the string
                cleaned_col = col.replace('[', '').replace(']', '').replace("'", '').replace('"', '')
                cleaned_columns.append(cleaned_col)
            
            df.columns = cleaned_columns
            df = df.fillna(0).replace('', 0)
            
            print(f"📋 Found {len(df)} weeks of data for {step_name}")
            print(f"🔧 Cleaned column names: {df.columns.tolist()}")
            
            # Debug: Show all available columns and sample data
            if not df.empty:
                print(f"📊 Sample data from first row:")
                for col in df.columns:
                    print(f"  '{col}': '{df.iloc[0][col]}'")

            # Ensure data is sorted by week start date in descending order (newest first)
            week_start_col = None
            for col in df.columns:
                if 'Week_Start_Date' in col:
                    week_start_col = col
                    break
            
            if week_start_col and not df.empty:
                # Convert to datetime and sort descending
                df[week_start_col] = pd.to_datetime(df[week_start_col])
                df = df.sort_values(by=week_start_col, ascending=False).reset_index(drop=True)
                print(f"✅ Data sorted by {week_start_col} in descending order (newest first)")

            # Process the data for weekly retention report
            retention_data = []
            
            # Debug: Print first few rows and columns
            print(f"DataFrame shape: {df.shape}")
            if not df.empty:
                print(f"Sample row data: {df.iloc[0].to_dict()}")
                
                # Debug: Print all dates to verify ordering
                print(f"🗓️  Final date order after sorting ({step_name}):")
                for idx in range(min(4, len(df))):
                    if week_start_col:
                        week_date = df.iloc[idx][week_start_col]
                        print(f"  Week {idx+1} (Row {idx}): {week_date}")
            
            for i, row in df.iterrows():
                # Access cleaned column names - after new cleaning method:
                # 'Calendar'[Week_Start_Date] → CalendarWeek_Start_Date  
                # 'Calendar'[Week Range] → CalendarWeek Range
                
                # Direct access using expected cleaned column names
                week_start_date = row.get('CalendarWeek_Start_Date', '')
                week_range = row.get('CalendarWeek Range', '')
                
                # Fallback: search for columns containing the keywords if direct access fails
                if not week_start_date:
                    for col in df.columns:
                        if 'Week_Start_Date' in col:
                            week_start_date = row.get(col, '')
                            break
                
                if not week_range:
                    for col in df.columns:
                        if 'Week Range' in col:
                            week_range = row.get(col, '')
                            break
                
                week_data = {
                    'week_start_date': str(week_start_date) if week_start_date else '',
                    'week_range': str(week_range) if week_range else '',
                    'initial_count': int(float(row.get('# Initials', 0))) if row.get('# Initials') else 0,
                    'retention_rate': f"{float(row.get('% Initial', 0)) * 100:.1f}%" if row.get('% Initial') else '0%',
                    'rebill_cycle1_count': int(float(row.get('# Rebills Cycle 1', 0))) if row.get('# Rebills Cycle 1') else 0,
                    'cycle1_retention_rate': f"{float(row.get('% Cycle 1', 0)) * 100:.1f}%" if row.get('% Cycle 1') else '0%'
                }
                retention_data.append(week_data)
                
                # Debug: Print first row's data
                if i == 0:
                    print(f"🎯 First week data: {week_data}")
                    print(f"🔍 Available columns: {list(row.keys())}")
                    print(f"🔍 Week start date: '{week_start_date}' (from column 'CalendarWeek_Start_Date')")
                    print(f"🔍 Week range: '{week_range}' (from column 'CalendarWeek Range')")

            print(f"✅ Successfully processed {len(retention_data)} weeks for {step_name}")
            return retention_data
            
        except Exception as e:
            print(f"❌ Error processing data for {step_name}: {e}")
            return None
    

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


    def convert_logo_to_base64(logo_path):
        """Convert logo image to base64 string for embedding in HTML"""
        try:
            if not os.path.exists(logo_path):
                print(f"⚠️  Logo file not found at: {logo_path}")
                return None
                
            with open(logo_path, "rb") as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                print(f"✅ Logo successfully converted to base64")
                return encoded_string
        except Exception as e:
            print(f"❌ Error converting logo to base64: {e}")
            return None

    def generate_weekly_retention_dashboard(all_retention_data, client_id):
        # If dataset is empty, skip generation
        if not all_retention_data:
            print("No retention data to display. Skipping HTML generation.")
            return None
        
        # Convert logo to base64
        current_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        logo_path = os.path.join(current_folder, "3x Icon.png")
        logo_base64 = convert_logo_to_base64(logo_path)
        
        with open('scripts/xpay_weekly/html/weekly-retention-template.html', 'r') as file:
            template = Template(file.read())
        
        html_content = template.render(
            retention_data=json.dumps(all_retention_data),
            client_id=client_id,
            generated_date=datetime.now().strftime('%B %d, %Y'),
            logo_base64=logo_base64 if logo_base64 else ""
        )
        
        output_folder = "scripts/xpay_weekly/html/output"  # Define the folder path
        os.makedirs(output_folder, exist_ok=True) 

        output_filename = os.path.join(output_folder, f"weekly_retention_{client_id}.html") 

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
        client_ids = [10008, 10009, 10010, 10011, 10012, 10013, 10014, 10016]
        print("No client IDs provided. Using default client IDs:", client_ids)
    else:
        print("Processing client IDs:", client_ids)
    
    # Loop over each client_id to fetch the respective report_id and product_id
    for client_id in client_ids:
        print(f"\n🚀 Starting weekly retention processing for Client {client_id}")
        
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

                        # Define the DAX queries for different campaign steps
                        queries = {
                            "step1": """
                            EVALUATE
                            TOPN(
                                4,
                                SUMMARIZECOLUMNS(
                                    'Calendar'[Week_Start_Date],
                                    'Calendar'[Week Range],
                                    FILTER(
                                        'Calendar',
                                        'Calendar'[Date] <= (TODAY() - 10)
                                    ),
                                    FILTER(
                                        trial_campaign,
                                        SEARCH("Step 1", trial_campaign[Campaign],, BLANK()) >= 1 &&
                                        NOT FIND("Step 1B", trial_campaign[Campaign],, BLANK())
                                    ),
                                    "# Initials", [Initials],
                                    "% Initial", [% Retention Initials],
                                    "# Rebills Cycle 1", CALCULATE([# Approved by Initial Date], order_details[Cycle] = 1),
                                    "% Cycle 1", [% Retention Cycle 1]
                                ),
                                'Calendar'[Week_Start_Date],
                                DESC
                            )
                            """,
                            "step1b": """
                            EVALUATE
                            TOPN(
                                4,
                                SUMMARIZECOLUMNS(
                                    'Calendar'[Week_Start_Date],
                                    'Calendar'[Week Range],
                                    FILTER(
                                        'Calendar',
                                        'Calendar'[Date] <= (TODAY() - 10)
                                    ),
                                    FILTER(
                                        trial_campaign,
                                        SEARCH("Step 1B", trial_campaign[Campaign],, BLANK()) >= 1 ||
                                        SEARCH("step1b", trial_campaign[Campaign],, BLANK()) >= 1
                                    ),
                                    "# Initials", [Initials],
                                    "% Initial", [% Retention Initials],
                                    "# Rebills Cycle 1", CALCULATE([# Approved by Initial Date], order_details[Cycle] = 1),
                                    "% Cycle 1", [% Retention Cycle 1]
                                ),
                                'Calendar'[Week_Start_Date],
                                DESC
                            )
                            """,
                            "step2": """
                            EVALUATE
                            TOPN(
                                4,
                                SUMMARIZECOLUMNS(
                                    'Calendar'[Week_Start_Date],
                                    'Calendar'[Week Range],
                                    FILTER(
                                        'Calendar',
                                        'Calendar'[Date] <= (TODAY() - 30)
                                    ),
                                    FILTER(
                                        trial_campaign,
                                        SEARCH("Step 2", trial_campaign[Campaign],, BLANK()) >= 1
                                    ),
                                    "# Initials", [Initials],
                                    "% Initial", [% Retention Initials],
                                    "# Rebills Cycle 1", CALCULATE([# Approved by Initial Date], order_details[Cycle] = 1),
                                    "% Cycle 1", [% Retention Cycle 1]
                                ),
                                'Calendar'[Week_Start_Date],
                                DESC
                            )
                            """,
                            "step3": """
                            EVALUATE
                            TOPN(
                                4,
                                SUMMARIZECOLUMNS(
                                    'Calendar'[Week_Start_Date],
                                    'Calendar'[Week Range],
                                    FILTER(
                                        'Calendar',
                                        'Calendar'[Date] <= (TODAY() - 30)
                                    ),
                                    FILTER(
                                        trial_campaign,
                                        SEARCH("Step 3", trial_campaign[Campaign],, BLANK()) >= 1
                                    ),
                                    "# Initials", [Initials],
                                    "% Initial", [% Retention Initials],
                                    "# Rebills Cycle 1", CALCULATE([# Approved by Initial Date], order_details[Cycle] = 1),
                                    "% Cycle 1", [% Retention Cycle 1]
                                ),
                                'Calendar'[Week_Start_Date],
                                DESC
                            )
                            """
                        }



                        try:
                            # Process campaign step queries one by one to improve performance
                            all_retention_data = {}
                            
                            # Execute queries sequentially with delay between each
                            for step_name, query in queries.items():
                                print(f"Processing {step_name} query for client {client_id}...")
                                payload = {"query": query}
                                
                                # Add a small delay between queries to reduce server load
                                if step_name != list(queries.keys())[0]:  # Skip delay for first query
                                    time.sleep(2)
                                
                                step_data = process_retention_data(url, headers, payload, client_id, step_name)
                                if step_data:
                                    all_retention_data[step_name] = step_data
                                    print(f"Successfully processed {step_name} - found {len(step_data)} weeks of data")
                                else:
                                    print(f"No data found for {step_name}")
                            
                            if all_retention_data:
                                print(f"Generating dashboard for client {client_id} with {len(all_retention_data)} campaign steps")
                                
                                # Generate the weekly retention dashboard
                                output_file = generate_weekly_retention_dashboard(all_retention_data, client_id)
                                if output_file:
                                    print(f"Weekly retention dashboard generated: {output_file}")
                                    
                                    image_folder = "images"
                                    os.makedirs(image_folder, exist_ok=True)
                                    
                                    png_path = os.path.join(image_folder, f"xpay_weekly_{client_id}.png")
                                    
                                    html_to_png(output_file, png_path)
                                    print(f"PNG for {client_id} saved at: {png_path}")
                                    
                                    metrics = {
                                        "client_id": str(client_id),
                                        "last_weekly_update_xpay": str(int(datetime.now().timestamp()))
                                    }
                                    
                                    push_to_customer_io(client_id, metrics)
                            else:
                                print(f"No retention data found for client {client_id}")
                                break
                            
                            # Configure Cloudinary (replace with your credentials)
                            # cloudinary.config(
                            #     cloud_name=config.get("production", "cloud_name"),  # Replace with your Cloudinary cloud name
                            #     api_key=config.get("production", "api_key"),        # Replace with your Cloudinary API key
                            #     api_secret=config.get("production", "api_secret")   # Replace with your Cloudinary API secret
                            # )
                            
                        #     # Call the function to upload images
                        #     upload_images_to_cloudinary()

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
    upload_images_to_cloudinary()

    images_directory = 'images'

    # Check if the directory exists
    if os.path.exists(images_directory):
        # Iterate over all files in the directory and remove them
        for file_name in os.listdir(images_directory):
            if file_name.lower().endswith(".png") and "xpay_weekly" in file_name:
                file_path = os.path.join(images_directory, file_name)
                # Only delete files (not subdirectories)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Deleted {file_path}")
    else:
        print(f"Directory {images_directory} does not exist.")
    
    print("Execution completed successfully, removed files from images directory")

    html_directory = 'scripts/xpay_weekly/html/output'
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
