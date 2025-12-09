import psycopg2
import pandas as pd
import configparser
import requests
import configparser
import psycopg2
import pandas as pd
import requests
import numpy as np
from decimal import Decimal
import os
from PIL import Image, ImageDraw, ImageFont
import io
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

    # Generates stats image
    def generate_and_save_image(client_metrics, client_id, logo_path):
        # Increase the image size to accommodate the larger padding
        img_width, img_height = 900, 1100  # Adjusted image size to fit all content
        img = Image.new('RGB', (img_width, img_height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)

            # Add gray background outside the border
        gray_color = (210, 235, 255)  # Light gray color
        draw.rectangle([0, 0, img_width, img_height], fill=gray_color)

        # Add curved border to the page
        border_padding = 60  # Space between content and border
        border_radius = 40  # Radius for the outer border
        draw.rounded_rectangle([border_padding, border_padding, img_width - border_padding, img_height - border_padding], 
                            radius=border_radius, outline=(255, 255, 255), width=3)

        # Add rounded white background inside the border
        inner_padding = border_padding + 3  # Add 3px to avoid overlap with the border line
        inner_radius = border_radius - 3  # Adjust radius to fit inside the border
        draw.rounded_rectangle([inner_padding, inner_padding, img_width - inner_padding, img_height - inner_padding], 
                            radius=inner_radius, fill=(255, 255, 255))

        
        # Load the logo image with transparency
        try:
            logo = Image.open(logo_path).convert("RGBA")  # Open as RGBA to retain transparency
            logo.thumbnail((200, 200))  # Resize logo to fit (adjust as needed)
        except IOError:
            print("Logo file not found or could not be opened.")
            return

        # Load fonts (You can use custom fonts for better appearance)
        try:
            title_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 55)  # Larger title font size
            subtitle_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 35)  # Bold subtitle font
            header_font = ImageFont.truetype("extras/ttf/segoe-ui-semibold.ttf", 35)  # Larger font for headers
            regular_font = ImageFont.truetype("extras/ttf/segoe-ui-semibold.ttf", 35)  # Regular font for values
        except IOError:
            # Fallback to default font if custom font is not available
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
            header_font = ImageFont.load_default()
            regular_font = ImageFont.load_default()

        
        # Get today's date and subtract 1 day
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime("%Y-%m-%d")  # Format the date as YYYY-MM-DD

        # Calculate the total width (logo + padding + title)
        padding = 30
        total_width = logo.width + padding 
        start_x = (img_width - total_width) // 2  # Center the combined width

        # Position the logo and title centered horizontally
        logo_y =60  # Y position for both logo and title (increased space from top)
        title_y = logo_y + (logo.height) // 2  # Vertically center title with logo

        # Paste logo
        img.paste(logo, (start_x, logo_y), logo)


        # Get today's date and subtract 1 day
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime("%b %d, %Y")  # Format as "Nov 06, 2024"

        # Create the subtitle text with the current date minus one day
        subtitle_text = f"Daily Stats: {formatted_date}"
        # Subtitle - "Daily Metrics" in bold and blue
        subtitle_bbox = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
        subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
        # midnight_blue = (25, 25, 112)   # Standard blue color (RGB)
        subtitle_y = logo_y + logo.height  # Position the subtitle below the title
        draw.text(((img_width - subtitle_width) / 2, subtitle_y), subtitle_text, font=subtitle_font, fill=(0, 0, 0))


        # Column headers for metrics
        header1 = "Count"
        header2 = "Metric"
        header3 = "Approval"
        header4 = "Revenue"
        header1_bbox = draw.textbbox((0, 0), header1, font=header_font)
        header2_bbox = draw.textbbox((0, 0), header2, font=header_font)
        header3_bbox = draw.textbbox((0, 0), header3, font=header_font)
        header4_bbox = draw.textbbox((0, 0), header4, font=header_font)
        header1_width = header1_bbox[2] - header1_bbox[0]
        header2_width = header2_bbox[2] - header2_bbox[0]
        header3_width = header3_bbox[2] - header3_bbox[0]
        header4_width = header4_bbox[2] - header4_bbox[0]

        # Increase spacing between columns
        column_spacing = 150
        total_table_width = header1_width + header2_width + header3_width + header4_width + 2 * column_spacing
        table_start_x = (img_width - total_table_width) // 2  # Center the table horizontally

        # # Draw headers with increased spacing
        header_y = subtitle_y + 80  # Position the header below the subtitle

        # Define row height (space between each metric row)
        row_height = 70  # Increase this value to create more space between metrics
        
        # Add metrics in tabular format
        y_position = header_y  # Starting position for the metrics

        metrics_data = [
            ("Joins", "joins","joins_approval", "joins_revenue"),
            ("Initials", "initials","initials_approval", "initials_revenue"),
            ("Rebills", "rebills","rebills_approval", "rebills_revenue"),
            ("FlexFactor", "flexfactor_total", "flexfactor_approval", "flexfactor_total_revenue"),
            ("Gross", "gross_revenue", None, "gross_revenue"),
            ("Chargebacks", "chargebacks_count", None, "chargebacks"),
            ("CS Refunds", "refund_cs_count", None, "refund_cs_amount"),
            ("Alert Refunds", "refund_alert_count", None, "refund_alert_amount"),
            ("Net", "net_revenue", None, "net_revenue")
        ]

        # Track the y-position of the last two metrics (Gross and Net) for line placement
        gross_y_position = None
        net_y_position = None
        
        for metric_name, count_key, approval_key, revenue_key in metrics_data:
            # Get the metric count and revenue values from client_metrics
            count_value = client_metrics.get(count_key, 0) or 0  # Replace None with 0
            approval_value = client_metrics.get(approval_key,   0) or 0
            revenue_value = client_metrics.get(revenue_key, 0) or 0  # Replace None with 0

            # Special handling for gross revenue count (sum of initials, rebills, and straight_sales)
            if metric_name == "Gross":
                count_value = ""
                gross_y_position = y_position

            # Special handling for net revenue count (sum of chargebacks and refunds)
            if metric_name == "Net":
                count_value = ""
                net_y_position = y_position
                
            
                

            # Format the count and revenue values safely
            # count_text = f"{count_value:,}" if count_value else ""
            # approval_text = f"{approval_value}%" if approval_value else ""
            # # Ensure revenue_value is numeric before applying the format
            # revenue_text = f"${revenue_value:,.0f}" if revenue_value is not None else "$0"
            
            # count_text = f"{int(count_value):,}" if count_value and str(count_value).isdigit() else ""

            if metric_name == "Net" or metric_name == "Gross":
                count_text = (
                    f"{int(count_value.replace(',', '')):,}"
                    if count_value and count_value.replace(",", "").isdigit()
                    else ""
                )
                
                #  Remove '%' before converting to float
                if approval_value:
                    approval_value_cleaned = approval_value.replace("%", "").strip()
                    approval_text = f"{float(approval_value_cleaned):.2f}%" if approval_value_cleaned.replace(".", "").isdigit() else ""
                else:
                    approval_text = ""
            else:
                count_text = (
                    f"{int(count_value.replace(',', '')):,}"
                    if count_value and count_value.replace(",", "").isdigit()
                    else "0"
                )
                
                #  Remove '%' before converting to float
                if approval_value:
                    approval_value_cleaned = approval_value.replace("%", "").strip()
                    approval_text = f"{float(approval_value_cleaned):.2f}%" if approval_value_cleaned.replace(".", "").isdigit() else ""
                else:
                    approval_text = "0"
            
            # revenue_text = f"${float(revenue_value):,.0f}" if revenue_value is not None and str(revenue_value).replace(".", "").isdigit() else "$0"
            revenue_text = (
                f"${float(revenue_value.replace(',', '')):,.0f}"
                if revenue_value is not None and revenue_value.replace(",", "").replace(".", "").replace("-", "").isdigit()
                else "$0"
            )
            
            if metric_name == "Alert Refunds" or metric_name == "CS Refunds" or metric_name == "Chargebacks":
                approval_text = ""
            
            if metric_name == "Joins":
                approval_text = "-"
                revenue_text = "-"
            
            # Ensure FlexFactor approval rate displays with % sign
            if metric_name == "FlexFactor":
                if approval_value and not str(approval_value).endswith('%'):
                    approval_text = f"{approval_value}%"
                elif approval_value:
                    approval_text = str(approval_value)  # Already has % sign


            # Calculate the width of the count and revenue text to center them
            count_bbox = draw.textbbox((0, 0), count_text, font=regular_font)
            approval_bbox = draw.textbbox((0, 0), approval_text, font=regular_font)
            revenue_bbox = draw.textbbox((0, 0), revenue_text, font=regular_font)
            

            count_width = count_bbox[2] - count_bbox[0] if count_value else 0
            approval_width = approval_bbox[2] - approval_bbox[0]
            revenue_width = revenue_bbox[2] - revenue_bbox[0]

            # Define specific spacings for each column gap
            count_metric_spacing = 50  # Reduced spacing between count and metric_name columns
            metric_revenue_spacing = column_spacing  # Keep original spacing between metric_name and revenue
            
            offset_all = 30
            offset = 350
            offset_rev = 60 
            # Right-align the count text within the first column
            # count_x = table_start_x + header1_width - count_width  # Right-align the count column

            
            # # Left-align the metric name in the second column
            # metric_name_x = table_start_x + header1_width + count_metric_spacing  # Left-align the metric name column
            
            # # Right-align the revenue text within the third column
            # revenue_x = table_start_x + header1_width + header2_width + 2 * metric_revenue_spacing + header3_width - revenue_width + offset_rev   # Right-align the revenue column

            # # Right-align approval_x within the bounds of metric_name_x and revenue_x
            # approval_x = revenue_x - (revenue_x - metric_name_x) / 3 + offset
            
            count_x = table_start_x + header1_width - count_width + offset_all # Right-align count column
            metric_name_x = table_start_x + header1_width + count_metric_spacing + offset_all  # Left-align metric name column
            revenue_x = (
                table_start_x
                + header1_width
                + header2_width
                + 2 * metric_revenue_spacing
                + header3_width
                - revenue_width
                + offset_rev
                + offset_all
            )  # Right-align revenue column

        
            # Define a consistent position for the approval column
            # Center the approval column between metric name and revenue columns
            approval_column_start = metric_name_x + 250  # Fixed offset from metric name (moved 30px right)
            approval_column_width = 120  # Fixed column width for approval
            
            # Center approval text within its column
            approval_x = approval_column_start + (approval_column_width - approval_width) // 2


            # Set color only for the specified metric names
            name_color = (105, 105, 105) if metric_name in ["Initials", "Rebills", "Joins", "FlexFactor", "Chargebacks", "Refund", "CS Refunds", "Alert Refunds"] else (0, 0, 0)
            name_font = subtitle_font if metric_name in ["Gross", "Net"] else regular_font  # Use bold font for "Gross" and "Net"
            
            revenue_color = None
            if metric_name in ["Initials", "Rebills", "Joins", "FlexFactor", "Gross", "Net"]:
                revenue_color = (40, 165, 40)  # Lime Green
            elif metric_name in ["Chargebacks", "Refund", "CS Refunds", "Alert Refunds"]:
                revenue_color = (220, 20, 60)  # Crimson Red

                
            count_color = (0, 0, 205)
            approval_color = (0, 0, 205)
            
            # Use consistent fonts for alignment
            approval_font = regular_font  # Use regular font for approval text to maintain alignment
            
            # Draw the count, metric name, and revenue text
            draw.text((count_x, y_position), count_text, font=regular_font, fill=count_color)  # Draw the count value
            draw.text((metric_name_x, y_position), metric_name, font=name_font, fill=name_color)  # Draw the metric name
            draw.text((approval_x, y_position), approval_text, font=approval_font, fill=approval_color)  # Draw the approval text
            draw.text((revenue_x, y_position), revenue_text, font=regular_font, fill=revenue_color)  # Draw the revenue value

            # Adjust y-position for the next row
            if metric_name == "Gross" or metric_name == "Net" :
                y_position += 120
            elif metric_name == "FlexFactor" or metric_name == "Alert Refunds":
                y_position += 100
            else:
                y_position += row_height    # Increase vertical spacing between rows

        # Add division lines above Gross and Net metrics only
        line_padding = 30  # Space above the metrics for the division line
        if gross_y_position is not None:
            # Division line above the Gross metric
            line_y = gross_y_position - line_padding
            draw.line([(table_start_x, line_y), (table_start_x + total_table_width, line_y)], fill=(0, 0, 0), width=1)
            
        
        if net_y_position is not None:
            # Division line above the Net metric
            line_y = net_y_position - line_padding
            draw.line([(table_start_x, line_y), (table_start_x + total_table_width, line_y)], fill=(0, 0, 0), width=1)
    
        # Ensure the 'images' directory exists
        if not os.path.exists('images'):
            os.makedirs('images')

        # Save the image to the 'images' folder with the client_id as the filename
        image_filename = f"images/{client_id}.png"
        img.save(image_filename)

        # img.save(image_filename.replace(".png", ".webp"), format="WEBP", quality=85)

        print(f"Image for client {client_id} saved at {image_filename}")
        
        return image_filename

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

    # Fetch all client ids from the "clients" table
    client_query = "select client_id from beast_insights_v2.crm_credentials where client_id = '10033';"
    clients_df = fetch_data_from_db(client_query)

    # Initialize an empty dictionary to store results by client
    client_results = {}

    # Initialize an empty list to store the final result
    result = []

    # If client data is available
    if clients_df is not None:
        # Loop over each client_id to fetch the respective report_id and product_id
        for client_id in clients_df['client_id']:
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
                            cookies = login_and_extract_cookies("dev_10033", "Dev@beastinsights/#2024")
                            
                            url = f"https://bi-api.beastinsights.co/api/page/executeQueries/{page_id}"

                            # Define the headers and payload for the API request
                            headers = {
                                'Content-Type': 'application/json',
                                'cookie': '; '.join([f'{key}={value}' for key, value in cookies.items()])
                            }

                            # Define the payload with Power BI DAX or M queries
                            
                            payload = {
                                "query": f"""
                                EVALUATE 
                                UNION(
                                    CALCULATETABLE( 
                                        ROW(
                                            "Joins", 
                                            CALCULATE(
                                                [# Approvals], 
                                                order_details[is_test] = "No",
                                                Calendar[Date] = TODAY() - 1,
                                                order_details[Sales Type] = "Straight Sales",
                                                payments[lender] <> "FlexFactor"
                                                
                                            )
                                        )
                                    ),
                                    ROW(
                                        "Joins Approval Rate (%)", 
                                        FORMAT(
                                            CALCULATE(
                                                DIVIDE([# Approvals], [# Attempts]), 
                                                order_details[is_test] = "No", 
                                                Calendar[Date] = TODAY() - 1, 
                                                order_details[Sales Type] = "Straight Sales",
                                                payments[lender] <> "FlexFactor"
                                            ), 
                                            "0.00%"
                                        )
                                    ),
                                    ROW(
                                        "Joins Order Value", 
                                        CALCULATE(
                                            SUM(order_details[order_total]), 
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1, 
                                            order_details[Sales Type] = "Straight Sales",
                                            order_details[order_status] = "Approved",
                                            payments[lender] <> "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "Approvals (Initials)", 
                                        CALCULATE(
                                            [# Approvals], 
                                            order_details[Sales Type] = "Initials", 
                                            order_details[Cycle] = 0, 
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1,
                                            payments[lender] <> "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "Initials Approval Rate (%)", 
                                        FORMAT(
                                            CALCULATE(
                                                DIVIDE([# Approvals], [# Attempts]), 
                                                order_details[Sales Type] = "Initials", 
                                                order_details[Cycle] = 0, 
                                                order_details[is_test] = "No", 
                                                Calendar[Date] = TODAY() - 1,
                                                payments[lender] <> "FlexFactor"
                                            ), 
                                            "0.00%"
                                        )
                                    ),
                                    ROW(
                                        "Initials Total Order Value", 
                                        CALCULATE(
                                            SUM(order_details[order_total]), 
                                            order_details[Sales Type] = "Initials", 
                                            order_details[Cycle] = 0, 
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1,  
                                            order_details[order_status] = "Approved",
                                            payments[lender] <> "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "Approvals (Rebills)", 
                                        CALCULATE(
                                            [# Approvals], 
                                            order_details[Sales Type] = "Rebills", 
                                            order_details[Cycle] >= 1,
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1,
                                            payments[lender] <> "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "Rebill Approval Rate (%)", 
                                        FORMAT(
                                            CALCULATE(
                                                DIVIDE([# Approvals], [# Attempts]), 
                                                order_details[Sales Type] = "Rebills", 
                                                order_details[Cycle] >= 1, 
                                                order_details[is_test] = "No", 
                                                Calendar[Date] = TODAY() - 1,
                                                payments[lender] <> "FlexFactor"
                                            ), 
                                            "0.00%"
                                        )
                                    ),
                                    ROW(
                                        "Rebill Total Order Value", 
                                        CALCULATE(
                                            SUM(order_details[order_total]), 
                                            order_details[Sales Type] = "Rebills", 
                                            order_details[Cycle] >= 1, 
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1,  
                                            order_details[order_status] = "Approved",
                                            payments[lender] <> "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "FlexFactor Total Approvals", 
                                        CALCULATE(
                                            [# Approvals], 
                                            order_details[is_test] = "No",
                                            Calendar[Date] = TODAY() - 1,
                                            payments[lender] = "FlexFactor"
                                        )
                                    ),
                                    ROW(
                                        "FlexFactor Approval Rate (%)", 
                                        FORMAT(
                                            CALCULATE(
                                                DIVIDE([# Approvals], [# Attempts]), 
                                                order_details[is_test] = "No",
                                                Calendar[Date] = TODAY() - 1,
                                                payments[lender] = "FlexFactor"
                                            ), 
                                            "0.00%"
                                        )
                                    ),
                                    ROW(
                                        "FlexFactor Total Order Value", 
                                        CALCULATE(
                                            SUM(order_details[order_total]), 
                                            order_details[is_test] = "No", 
                                            Calendar[Date] = TODAY() - 1, 
                                            order_details[order_status] = "Approved",
                                            payments[lender] = "FlexFactor"
                                        )
                                    )
                                )
                                """
                            }

                            try:
                                # Make the API request
                                # response = requests.post(url, headers=headers, json=payload, timeout=300)

                                # response.raise_for_status()  # Check if request was successful
                                
                                max_retries = 5  # Set a limit to avoid infinite loops
                                retry_delay = 5  # Delay between retries in seconds
                                attempt = 0

                                while attempt < max_retries:
                                    try:
                                        response = requests.post(url, headers=headers, json=payload, timeout=300)
                                        response.raise_for_status()  # Raise exception for HTTP errors

                                        if response.status_code == 200:
                                            print("Request successful:", response.json())  # Process the response if needed
                                            break  # Exit the loop
                                        else:
                                            print(f"Attempt {attempt + 1}: Received status code {response.status_code}. Retrying...")

                                    except requests.exceptions.RequestException as e:
                                        print(f"Attempt {attempt + 1}: Error - {e}. Retrying...")

                                    attempt += 1
                                    time.sleep(retry_delay)  # Wait before retrying

                                if attempt == max_retries:
                                    print("Max retries reached. Request failed.")
                                
                                data = response.json()  # Assuming JSON response

                                # Extract rows from the JSON structure
                                rows = data['data']['results'][0]['tables'][0]['rows']

                                # Flatten the list of dictionaries
                                flat_list = [list(d.values())[0] for d in rows]

                                # Create the DataFrame with required format - now with 12 rows (9 original + 3 FlexFactor)
                                df = pd.DataFrame({
                                    'Metric': [
                                        'Joins', 'Joins Approval %', 'Joins Value',
                                        'Initials', 'Initials Approval %', 'Initials Value', 
                                        'Rebill', 'Rebill Approval %', 'Rebill Value',
                                        'FlexFactor Total', 'FlexFactor Approval %', 'FlexFactor Total Value'
                                    ],
                                    'Value': flat_list
                                })

                                # Display DataFrame
                                print(df)
                                
                                # Extract values into separate variables for regular (non-FlexFactor) data
                                joins_count = flat_list[0]
                                joins_approval = flat_list[1] 
                                joins_value = flat_list[2]

                                initials_count = flat_list[3]
                                initials_approval = flat_list[4]
                                initials_value = flat_list[5]

                                rebill_count = flat_list[6]
                                rebill_approval = flat_list[7]
                                rebill_value = flat_list[8]
                                
                                # Extract FlexFactor combined values
                                flexfactor_total_count = flat_list[9]
                                flexfactor_approval = flat_list[10]
                                flexfactor_total_value = flat_list[11]
                                
                                joins_approval = joins_approval or 0
                                

                                
                                # Print extracted variables
                                print(f"Joins -> Count: {joins_count}, Approval %: {joins_approval}, Value: {joins_value}")
                                print(f"Initials -> Count: {initials_count}, Approval %: {initials_approval}, Value: {initials_value}")
                                print(f"Rebill -> Count: {rebill_count}, Approval %: {rebill_approval}, Value: {rebill_value}")
                                print(f"FlexFactor Total -> Count: {flexfactor_total_count}, Approval %: {flexfactor_approval}, Value: {flexfactor_total_value}")
                                
                                

                            except requests.exceptions.RequestException as e:
                                print(f"API request failed for page_id {page_id}: {e}")
                                summary.add_failure(
                                    f"Daily Metrics refresh failed for client {client_id}", 
                                    f"Daily Metrics refresh failed for client {client_id} : {str(e)}"
                                )
            
            try:
                db_metrics_query = f"SELECT  gross_revenue, net_revenue, chargebacks_count, chargebacks, refund_count, refund, refund_alert_count, refund_cs_count, refund_alert_amount, refund_cs_amount FROM beast_insights_v2.daily_data_sync where client_id = {client_id} and date = CURRENT_DATE - INTERVAL '1 day';"          
                db_metrics = fetch_data_from_db(db_metrics_query) 
                print(db_metrics)   
                # Ensure df_metrics exists
                if not db_metrics.empty:
                    gross_revenue = db_metrics.loc[0, 'gross_revenue']
                    net_revenue = db_metrics.loc[0, 'net_revenue']
                    chargebacks_count = db_metrics.loc[0, 'chargebacks_count']
                    chargebacks = db_metrics.loc[0, 'chargebacks']
                    refund_count = db_metrics.loc[0, 'refund_count']
                    refund = db_metrics.loc[0, 'refund']
                    refund_cs_count = db_metrics.loc[0, 'refund_cs_count']
                    refund_cs_amount = db_metrics.loc[0, 'refund_cs_amount']
                    refund_alert_count = db_metrics.loc[0, 'refund_alert_count']
                    refund_alert_amount = db_metrics.loc[0, 'refund_alert_amount']
                else:
                    chargebacks_count = chargebacks = refund_count = refund = net_revenue = gross_revenue = refund_cs_count = refund_cs_amount = refund_alert_count = refund_alert_amount = None

                net_revenue = round(net_revenue) if net_revenue is not None else 0
                gross_revenue = round(gross_revenue) if gross_revenue is not None else 0
                chargebacks = round(chargebacks) if chargebacks is not None else 0
                chargebacks_count = round(chargebacks_count) if chargebacks_count is not None else 0
                refund = round(refund) if refund is not None else 0
                refund_count = round(refund_count) if refund_count is not None else 0
                refund_cs_count = round(refund_cs_count) if refund_cs_count is not None else 0
                refund_cs_amount = round(refund_cs_amount) if refund_cs_amount is not None else 0
                refund_alert_count = round(refund_alert_count) if refund_alert_count is not None else 0
                refund_alert_amount = round(refund_alert_amount) if refund_alert_amount is not None else 0
                
                net_revenue = f"{net_revenue:,}" if net_revenue is not None else 0
                gross_revenue = f"{gross_revenue:,}" if gross_revenue is not None else 0
                chargebacks = f"{chargebacks:,}" if chargebacks is not None else 0
                chargebacks_count = f"{chargebacks_count:,}" if chargebacks_count is not None else 0
                refund = f"{refund:,}" if refund is not None else 0
                refund_count = f"{refund_count:,}" if refund_count is not None else 0
                refund_cs_count = f"{refund_cs_count:,}" if refund_cs_count is not None else 0
                refund_cs_amount = f"{refund_cs_amount:,}" if refund_cs_amount is not None else 0
                refund_alert_count = f"{refund_alert_count:,}" if refund_alert_count is not None else 0
                refund_alert_amount = f"{refund_alert_amount:,}" if refund_alert_amount is not None else 0
                
                net_revenue = str(net_revenue)
                gross_revenue = str(gross_revenue)
                chargebacks = str(chargebacks)
                chargebacks_count = str(chargebacks_count)
                refund = str(refund)
                refund_count = str(refund_count)
                refund_cs_count = str(refund_cs_count)
                refund_cs_amount = str(refund_cs_amount)
                refund_alert_count = str(refund_alert_count)
                refund_alert_amount = str(refund_alert_amount)
                
                
                # Print extracted values
                print(f"Refund: {gross_revenue}")
                print(f"Chargebacks Count: {chargebacks_count}")
                print(f"Chargebacks: {chargebacks}")
                print(f"Refund Count: {refund_count}")
                print(f"Refund: {refund}")
                print(f"Refund: {net_revenue}")
                print(f"refund_cs_count: {refund_cs_count}")
                print(f"refund_cs_amount: {refund_cs_amount}")
                print(f"refund_alert_count: {refund_alert_count}")
                print(f"refund_alert_amount: {refund_alert_amount}")
                
                
                # Create dictionary with all variables and their values
                data = {
                    "Metric": [
                        "Joins Count", "Joins Approval %", "Joins Value",
                        "Initials Count", "Initials Approval %", "Initials Value",
                        "Rebill Count", "Rebill Approval %", "Rebill Value",
                        "FlexFactor Total Count", "FlexFactor Approval %", "FlexFactor Total Value",
                        "Gross", "Net",
                        "Chargebacks Count", "Chargebacks", "Refund Count", "Refund", "refund_cs_count", "refund_cs_amount", "refund_alert_count", "refund_alert_amount"
                    ],
                    "Value": [
                        joins_count, joins_approval, joins_value,
                        initials_count, initials_approval, initials_value,
                        rebill_count, rebill_approval, rebill_value,
                        flexfactor_total_count, flexfactor_approval, flexfactor_total_value,
                        gross_revenue, net_revenue,
                        chargebacks_count, chargebacks, refund_count, refund, refund_cs_count, refund_cs_amount, refund_alert_count, refund_alert_amount
                    ]
                }

                # Convert to DataFrame
                df_metrics = pd.DataFrame(data)
                df_metrics['Value'] = df_metrics['Value'].replace('', np.nan).fillna(0)
                
                joins_value = round(float(joins_value)) if joins_value is not None else 0 # Convert to float and round
                joins_value = f"{joins_value:,}" if joins_value is not None else 0
                joins_value = str(joins_value) if joins_value is not None else 0  # Convert back to string

                
                initials_value = round(float(initials_value)) if initials_value is not None else 0  # Convert to float and round
                initials_value = f"{initials_value:,}" if initials_value is not None else 0
                initials_value = str(initials_value) if initials_value is not None else 0  # Convert back to string

                
                rebill_value = round(float(rebill_value)) if rebill_value is not None else 0 # Convert to float and round
                rebill_value = f"{rebill_value:,}" if rebill_value is not None else 0
                rebill_value = str(rebill_value) if rebill_value is not None else 0 # Convert back to string

                joins_count = round(float(joins_count)) if joins_count is not None else 0
                joins_count = f"{joins_count:,}" if joins_count is not None else 0
                joins_count = str(joins_count) if joins_count is not None else 0
                
                initials_count = round(float(initials_count)) if initials_count is not None else 0
                initials_count = f"{initials_count:,}" if initials_count is not None else 0
                initials_count = str(initials_count) if initials_count is not None else 0
                
                rebill_count = round(float(rebill_count)) if rebill_count is not None else 0
                rebill_count = f"{rebill_count:,}" if rebill_count is not None else 0
                rebill_count = str(rebill_count) if rebill_count is not None else 0
                
                # Format FlexFactor combined values
                flexfactor_total_value = round(float(flexfactor_total_value)) if flexfactor_total_value is not None else 0
                flexfactor_total_value = f"{flexfactor_total_value:,}" if flexfactor_total_value is not None else 0
                flexfactor_total_value = str(flexfactor_total_value) if flexfactor_total_value is not None else 0
                
                flexfactor_total_count = round(float(flexfactor_total_count)) if flexfactor_total_count is not None else 0
                flexfactor_total_count = f"{flexfactor_total_count:,}" if flexfactor_total_count is not None else 0
                flexfactor_total_count = str(flexfactor_total_count) if flexfactor_total_count is not None else 0
                
                # FlexFactor approval rate is already formatted as percentage from DAX query
                flexfactor_approval = flexfactor_approval if flexfactor_approval is not None else "0.00%"
                
                metrics = {
                    "db_id": client_id,  # Set db_id as the identifier in the metrics payload
                    "joins": joins_count,
                    "joins_revenue": joins_value,
                    "joins_approval": joins_approval,
                    
                    "initials": initials_count,
                    "initials_revenue": initials_value,
                    "initials_approval": initials_approval,
                    
                    "rebills": rebill_count,
                    "rebills_revenue": rebill_value,
                    "rebills_approval": rebill_approval,

                    "flexfactor_total": flexfactor_total_count,
                    "flexfactor_approval": flexfactor_approval,
                    "flexfactor_total_revenue": flexfactor_total_value,

                    "gross_revenue": gross_revenue,
                    "net_revenue": net_revenue,

                    "chargebacks_count": chargebacks_count,
                    "chargebacks": chargebacks,

                    "refund_count": refund_count,
                    "refund": refund,
                    
                    "refund_cs_count" : refund_cs_count,
                    "refund_cs_amount" : refund_cs_amount,
                    "refund_alert_count" : refund_alert_count,
                    "refund_alert_amount" : refund_alert_amount,

                    "last_daily_update": int(datetime.now().timestamp())  # Unix timestamp
                }


                
                # Display DataFrame
                print(df_metrics)
                
                current_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                file_name = "3x Icon.png"
                logo = os.path.join(current_folder, file_name)
                
                image_file = generate_and_save_image(metrics, client_id, logo)

                # Add the new date metric to the existing metrics dictionary
                metrics["yesterday_date"] = (datetime.now() - timedelta(days=1)).strftime('%b %d, %Y')
                
                push_to_customer_io(client_id, metrics)
                
                # Configure Cloudinary (replace with your credentials)
                cloudinary.config(
                    cloud_name=config.get("production", "cloud_name"),  # Replace with your Cloudinary cloud name
                    api_key=config.get("production", "api_key"),        # Replace with your Cloudinary API key
                    api_secret=config.get("production", "api_secret")   # Replace with your Cloudinary API secret
                )
                
                # Call the function to upload images
                upload_images_to_cloudinary()

                # upload_images_to_drive(google_drive_folder_id)
                # Directory path to the 'images' folder
                images_directory = 'images'

                # Check if the directory exists
                if os.path.exists(images_directory):
                    # Iterate over all files in the directory and remove them
                    for file_name in os.listdir(images_directory):
                        file_path = os.path.join(images_directory, file_name)
                        # Only delete files (not subdirectories)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            print(f"Deleted {file_name}")
                else:
                    print(f"Directory {images_directory} does not exist.")
                
                print("Execution completed successfully, removed files from images directory")
            except Exception as e:
                print("error")
                summary.add_failure(
                    f"Daily Metrics refresh failed for client {client_id}", 
                    f"Daily Metrics refresh failed for client {client_id} : {str(e)}"
                )
                
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
