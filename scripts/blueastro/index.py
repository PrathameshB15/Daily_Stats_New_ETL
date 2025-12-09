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

    # Generate stats image
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
            logo.thumbnail((150, 150))  # Resize logo to fit (adjust as needed)
        except IOError:
            print("Logo file not found or could not be opened.")
            return

        # Load fonts (You can use custom fonts for better appearance)
        try:
            title_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 55)  # Larger title font size
            subtitle_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 30)  # Bold subtitle font
            header_font = ImageFont.truetype("extras/ttf/segoe-ui-semibold.ttf", 30)  # Larger font for headers
            regular_font = ImageFont.truetype("extras/ttf/segoe-ui-semibold.ttf", 25)  # Regular font for values
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
        logo_y = 50  # Y position for both logo and title (increased space from top)
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
        subtitle_y = logo_y + logo.height - 20  # Position the subtitle below the title
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
        row_height = 55  # Increase this value to create more space between metrics
        
        # Add metrics in tabular format
        y_position = header_y  # Starting position for the metrics

        metrics_data = [
            ("Initials VP1", "initials_vp1","initials_vp1_approval", "initials_vp1_revenue"),
            ("Initials MP1", "initials_mp1","initials_mp1_approval", "initials_mp1_revenue"),
            ("Rebills VP2", "rebill_vp2","rebill_vp2_approval", "rebill_vp2_revenue"),
            ("Rebills MP2", "rebill_mp2","rebill_mp2_approval", "rebill_mp2_revenue"),
            ("Fitness P2 VISA", "fitness_p2_visa","fitness_p2_visa_approval", "fitness_p2_visa_revenue"),
            ("Fitness P2 MC", "fitness_p2_mc","fitness_p2_mc_approval", "fitness_p2_mc_revenue"),
            ("Ebook P2", "ebook_p2","ebook_p2_approval", "ebook_p2_revenue"),
            ("Other Products", "other_products","other_products_approval", "other_products_revenue"),
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

            # Define a fixed position for the approval column
            approval_column_offset = header1_width + count_metric_spacing + (header2_width / 2)

            # Calculate the approval text width
            approval_width = approval_bbox[2] - approval_bbox[0]
            
            # Calculate approval_x based on a fixed offset
            approval_x = table_start_x + approval_column_offset + offset - approval_width + offset_all

            # Set color only for the specified metric names
            name_color = (105, 105, 105) if metric_name in ["Initials VP1", "Initials MP1", "Rebills VP2", "Rebills MP2", "Fitness P2 VISA", "Fitness P2 MC", "Ebook P2", "Other Products", "Chargebacks", "Refund", "CS Refunds", "Alert Refunds"] else (0, 0, 0)
            name_font = subtitle_font if metric_name in ["Gross", "Net"] else regular_font  # Use bold font for "Gross" and "Net"
            
            # Set revenue color based on the metric name
            revenue_color = None
            if metric_name in ["Initials VP1", "Initials MP1", "Rebills VP2", "Rebills MP2", "Fitness P2 VISA", "Fitness P2 MC", "Ebook P2", "Other Products", "Gross", "Net"]:
                revenue_color = (40, 165, 40)  # Lime Green
            elif metric_name in ["Chargebacks", "Refund", "Alert Refunds", "CS Refunds"]:
                revenue_color = (220, 20, 60)  # Crimson Red

            
            count_color = (0, 0, 205)
            approval_color = (0, 0, 205)
            
            # Draw the count, metric name, and revenue text
            draw.text((count_x, y_position), count_text, font=regular_font, fill=count_color)  # Draw the count value
            draw.text((metric_name_x, y_position), metric_name, font=name_font, fill=name_color)  # Draw the metric name centered
            draw.text((approval_x, y_position), approval_text, font=name_font, fill=approval_color)  # Draw the metric name centered
            draw.text((revenue_x, y_position), revenue_text, font=regular_font, fill=revenue_color)  # Draw the revenue value

            # Adjust y-position for the next row
            if metric_name == "Gross" or metric_name == "Net" :
                y_position += 70
            elif metric_name == "Other Products" or metric_name == "Alert Refunds":
                y_position += 70
            else:
                y_position += row_height    # Increase vertical spacing between rows

        # Add division lines above Gross and Net metrics only
        line_padding = 10  # Space above the metrics for the division line
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

    # Push the metric data to customer.io
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

    # Upload the final image to cloud platform
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
    client_query = "select client_id from beast_insights_v2.crm_credentials where client_id = '10030';"
    clients_df = fetch_data_from_db(client_query)

    # Initialize an empty dictionary to store results by client
    client_results = {}

    # Initialize an empty list to store the final result
    result = []

    # If client data is available
    if clients_df is not None:
        # Loop over each client_id to fetch the respective report_id and product_id
        for client_id in clients_df['client_id']:
            try:
                # Execute SQL query to get blueastro metrics directly from database
                blueastro_query = f"""
                WITH blueastro_raw AS (
                    SELECT
                        o.product_id,
                        o.cycle,
                        o.attempt,
                        o.gateway_id,
                        o.order_total,
                        o.is_approved,
                        o.bin,
                        c.campaign_type,
                        bl.card
                    FROM data.orders_10030 o
                    LEFT JOIN public.campaigns c
                        ON c.client_id = o.client_id AND c.campaign_id = o.campaign_id
                    LEFT JOIN public.bin_lookup bl
                        ON bl.bin = o.bin
                    WHERE o.date_of_sale = CURRENT_DATE - INTERVAL '1 day'
                    AND (o.is_test = false OR o.is_test IS NULL)
                    AND o.end_date = DATE '9999-12-31' 
                    AND (o.is_exclude = false OR o.is_exclude IS NULL)
                ),
                vp1_initials AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 2 AND COALESCE(campaign_type, '') NOT IN ('Prepaid', 'DP', 'Scrub')
                ),
                mp1_initials AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 7 AND COALESCE(campaign_type, '') NOT IN ('Prepaid', 'DP', 'Scrub')
                ),
                vp2_rebills AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 3 AND cycle = 1 AND attempt = 1 AND campaign_type = 'Limited'
                ),
                mp2_rebills AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 8 AND cycle = 1 AND attempt = 1 AND campaign_type = 'Limited'
                ),
                fitness_p2_visa AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 13 AND cycle = 1 AND attempt = 1 AND card = 'VISA' AND COALESCE(campaign_type, '') NOT IN ('Prepaid', 'DP', 'Scrub')
                ),
                fitness_p2_mc AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 13 AND cycle = 1 AND attempt = 1 AND card = 'MASTERCARD' AND COALESCE(campaign_type, '') NOT IN ('Prepaid', 'DP', 'Scrub')
                ),
                ebook_p2 AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id = 16 AND cycle = 1 AND attempt = 1 AND COALESCE(campaign_type, '') NOT IN ('Prepaid', 'DP', 'Scrub')
                ),
                other_products AS (
                    SELECT COUNT(*) FILTER (WHERE is_approved = true) as approvals, COUNT(*) as attempts,
                        SUM(CASE WHEN is_approved = true THEN order_total ELSE 0 END) as revenue
                    FROM blueastro_raw WHERE product_id NOT IN (1, 2, 3, 7, 8, 13, 16, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 41, 43)
                )
                SELECT json_build_object(
                    'initials_vp1', (SELECT COALESCE(approvals, 0) FROM vp1_initials),
                    'initial_vp1_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM vp1_initials),
                    'total_vp1_order_value', (SELECT COALESCE(revenue, 0) FROM vp1_initials),
                    'initials_mp1', (SELECT COALESCE(approvals, 0) FROM mp1_initials),
                    'initial_mp1_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM mp1_initials),
                    'total_mp1_order_value', (SELECT COALESCE(revenue, 0) FROM mp1_initials),
                    'rebill_vp2', (SELECT COALESCE(approvals, 0) FROM vp2_rebills),
                    'rebill_vp2_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM vp2_rebills),
                    'rebill_vp2_order_value', (SELECT COALESCE(revenue, 0) FROM vp2_rebills),
                    'rebill_mp2', (SELECT COALESCE(approvals, 0) FROM mp2_rebills),
                    'rebill_mp2_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM mp2_rebills),
                    'rebill_mp2_order_value', (SELECT COALESCE(revenue, 0) FROM mp2_rebills),
                    'fitness_p2_visa', (SELECT COALESCE(approvals, 0) FROM fitness_p2_visa),
                    'fitness_p2_visa_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM fitness_p2_visa),
                    'fitness_p2_visa_order_value', (SELECT COALESCE(revenue, 0) FROM fitness_p2_visa),
                    'fitness_p2_mc', (SELECT COALESCE(approvals, 0) FROM fitness_p2_mc),
                    'fitness_p2_mc_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM fitness_p2_mc),
                    'fitness_p2_mc_order_value', (SELECT COALESCE(revenue, 0) FROM fitness_p2_mc),
                    'ebook_p2', (SELECT COALESCE(approvals, 0) FROM ebook_p2),
                    'ebook_p2_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM ebook_p2),
                    'ebook_p2_order_value', (SELECT COALESCE(revenue, 0) FROM ebook_p2),
                    'other_products', (SELECT COALESCE(approvals, 0) FROM other_products),
                    'other_products_approval_rate', (SELECT COALESCE(ROUND(approvals::NUMERIC / NULLIF(attempts, 0) * 100, 2), 0) FROM other_products),
                    'other_products_order_value', (SELECT COALESCE(revenue, 0) FROM other_products)
                ) as blueastro_stats;
                """
                
                # Execute the query
                df = fetch_data_from_db(blueastro_query)
                
                if df is not None and not df.empty:
                    print(f"Blueastro metrics for client {client_id}:")
                    print(df)
                    
                    # Extract the JSON object from the blueastro_stats column
                    blueastro_json = df.loc[0, 'blueastro_stats']
                    
                    # Extract values from the JSON object with default values
                    initials_vp1_count = blueastro_json.get('initials_vp1', 0)
                    initials_vp1_approval = f"{blueastro_json.get('initial_vp1_approval_rate', 0):.2f}%"
                    initials_vp1_value = blueastro_json.get('total_vp1_order_value', 0)
                    
                    initials_mp1_count = blueastro_json.get('initials_mp1', 0)
                    initials_mp1_approval = f"{blueastro_json.get('initial_mp1_approval_rate', 0):.2f}%"
                    initials_mp1_value = blueastro_json.get('total_mp1_order_value', 0)

                    rebill_vp2_count = blueastro_json.get('rebill_vp2', 0)
                    rebill_vp2_approval = f"{blueastro_json.get('rebill_vp2_approval_rate', 0):.2f}%"
                    rebill_vp2_value = blueastro_json.get('rebill_vp2_order_value', 0)

                    rebill_mp2_count = blueastro_json.get('rebill_mp2', 0)
                    rebill_mp2_approval = f"{blueastro_json.get('rebill_mp2_approval_rate', 0):.2f}%"
                    rebill_mp2_value = blueastro_json.get('rebill_mp2_order_value', 0)
                    
                    fitness_p2_visa_count = blueastro_json.get('fitness_p2_visa', 0)
                    fitness_p2_visa_approval = f"{blueastro_json.get('fitness_p2_visa_approval_rate', 0):.2f}%"
                    fitness_p2_visa_value = blueastro_json.get('fitness_p2_visa_order_value', 0)

                    fitness_p2_mc_count = blueastro_json.get('fitness_p2_mc', 0)
                    fitness_p2_mc_approval = f"{blueastro_json.get('fitness_p2_mc_approval_rate', 0):.2f}%"
                    fitness_p2_mc_value = blueastro_json.get('fitness_p2_mc_order_value', 0)

                    ebook_p2_count = blueastro_json.get('ebook_p2', 0)
                    ebook_p2_approval = f"{blueastro_json.get('ebook_p2_approval_rate', 0):.2f}%"
                    ebook_p2_value = blueastro_json.get('ebook_p2_order_value', 0)
                    
                    other_products_count = blueastro_json.get('other_products', 0)
                    other_products_approval = f"{blueastro_json.get('other_products_approval_rate', 0):.2f}%"
                    other_products_value = blueastro_json.get('other_products_order_value', 0)
                    
                    # Print extracted variables
                    print(f"Initials VP1 -> Count: {initials_vp1_count}, Approval %: {initials_vp1_approval}, Value: {initials_vp1_value}")
                    print(f"Initials MP1 -> Count: {initials_mp1_count}, Approval %: {initials_mp1_approval}, Value: {initials_mp1_value}")
                    print(f"Rebill VP2 -> Count: {rebill_vp2_count}, Approval %: {rebill_vp2_approval}, Value: {rebill_vp2_value}")
                    print(f"Rebill MP2 -> Count: {rebill_mp2_count}, Approval %: {rebill_mp2_approval}, Value: {rebill_mp2_value}")
                    print(f"Fitness P2 VISA -> Count: {fitness_p2_visa_count}, Approval %: {fitness_p2_visa_approval}, Value: {fitness_p2_visa_value}")
                    print(f"Fitness P2 MC -> Count: {fitness_p2_mc_count}, Approval %: {fitness_p2_mc_approval}, Value: {fitness_p2_mc_value}")
                    print(f"Ebook P2 -> Count: {ebook_p2_count}, Approval %: {ebook_p2_approval}, Value: {ebook_p2_value}")
                    print(f"Other Products -> Count: {other_products_count}, Approval %: {other_products_approval}, Value: {other_products_value}")
                else:
                    # Set default values if no data found
                    initials_vp1_count = initials_mp1_count = rebill_vp2_count = rebill_mp2_count = 0
                    fitness_p2_visa_count = fitness_p2_mc_count = ebook_p2_count = other_products_count = 0
                    initials_vp1_approval = initials_mp1_approval = rebill_vp2_approval = rebill_mp2_approval = "0.00%"
                    fitness_p2_visa_approval = fitness_p2_mc_approval = ebook_p2_approval = other_products_approval = "0.00%"
                    initials_vp1_value = initials_mp1_value = rebill_vp2_value = rebill_mp2_value = 0
                    fitness_p2_visa_value = fitness_p2_mc_value = ebook_p2_value = other_products_value = 0

            except Exception as e:
                print(f"Error fetching blueastro metrics for client {client_id}: {e}")
                summary.add_failure(
                    f"Daily Metrics refresh failed for client {client_id}", 
                    f"Daily Metrics refresh failed for client {client_id} : {str(e)}"
                )
                # Set default values on error
                initials_vp1_count = initials_mp1_count = rebill_vp2_count = rebill_mp2_count = 0
                fitness_p2_visa_count = fitness_p2_mc_count = ebook_p2_count = other_products_count = 0
                initials_vp1_approval = initials_mp1_approval = rebill_vp2_approval = rebill_mp2_approval = "0.00%"
                fitness_p2_visa_approval = fitness_p2_mc_approval = ebook_p2_approval = other_products_approval = "0.00%"
                initials_vp1_value = initials_mp1_value = rebill_vp2_value = rebill_mp2_value = 0
                fitness_p2_visa_value = fitness_p2_mc_value = ebook_p2_value = other_products_value = 0

            try:
                # Get yesterday's date in YYYY-MM-DD format
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                
                db_metrics_query = f"""
                SELECT
                    -- gross revenue (total)
                    COALESCE(SUM(revenue), 0) AS gross_revenue,

                    -- net revenue calculation (gross - chargebacks - refund_alert_amount - refund_cs_amount)
                    (
                        COALESCE(SUM(revenue),0)
                        - COALESCE(SUM(cb_cb_date_dollar),0)
                        - COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date_dollar END),0)
                        - COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date_dollar END),0)
                    ) AS net_revenue,

                    -- chargebacks
                    COALESCE(SUM(cb_cb_date), 0) AS chargebacks_count,
                    COALESCE(SUM(cb_cb_date_dollar), 0) AS chargebacks,

                    -- total refunds (sum of Alert + CS counts)
                    (COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date END), 0) + 
                     COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date END), 0)) AS refund_count,
                    
                    -- total refund amount (sum of Alert + CS amounts)
                    (COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date_dollar END), 0) + 
                     COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date_dollar END), 0)) AS refund,

                    -- refund splits (Alert / CS)
                    COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date END), 0) AS refund_alert_count,
                    COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date_dollar END), 0) AS refund_alert_amount,

                    COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date END), 0) AS refund_cs_count,
                    COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date_dollar END), 0) AS refund_cs_amount

                FROM "reporting"."order_summary_{client_id}"
                WHERE date = '{yesterday}';
                """
                
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
                    chargebacks_count = chargebacks = refund_count = refund = gross_revenue = net_revenue = refund_cs_count = refund_cs_amount = refund_alert_count = refund_alert_amount = None
                
                # # Calculate Net
                # net = gross - to_float(chargebacks) - to_float(refund)
                
                gross_revenue = round(gross_revenue) if gross_revenue is not None else 0
                net_revenue = round(net_revenue) if net_revenue is not None else 0
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
                print(f"Gross: {gross_revenue}")
                print(f"Chargebacks Count: {chargebacks_count}")
                print(f"Chargebacks: {chargebacks}")
                print(f"Refund Count: {refund_count}")
                print(f"Refund: {refund}")
                print(f"refund_cs_count: {refund_cs_count}")
                print(f"refund_cs_amount: {refund_cs_amount}")
                print(f"refund_alert_count: {refund_alert_count}")
                print(f"refund_alert_amount: {refund_alert_amount}")
                print(f"Net: {net_revenue}")
                
                initials_vp1_value = round(float(initials_vp1_value)) if initials_vp1_value is not None else 0  # Convert to float and round
                initials_vp1_value = f"{initials_vp1_value:,}" if initials_vp1_value is not None else 0
                initials_vp1_value = str(initials_vp1_value) if initials_vp1_value is not None else 0  # Convert back to string
                
                initials_mp1_value = round(float(initials_mp1_value)) if initials_mp1_value is not None else 0  # Convert to float and round
                initials_mp1_value = f"{initials_mp1_value:,}" if initials_mp1_value is not None else 0
                initials_mp1_value = str(initials_mp1_value) if initials_mp1_value is not None else 0  # Convert back to string
                
                rebill_vp2_value = round(float(rebill_vp2_value)) if rebill_vp2_value is not None else 0  # Convert to float and round
                rebill_vp2_value = f"{rebill_vp2_value:,}" if rebill_vp2_value is not None else 0
                rebill_vp2_value = str(rebill_vp2_value) if rebill_vp2_value is not None else 0  # Convert back to string
                
                rebill_mp2_value = round(float(rebill_mp2_value)) if rebill_mp2_value is not None else 0  # Convert to float and round
                rebill_mp2_value = f"{rebill_mp2_value:,}" if rebill_mp2_value is not None else 0
                rebill_mp2_value = str(rebill_mp2_value) if rebill_mp2_value is not None else 0  # Convert back to string

                fitness_p2_visa_value = round(float(fitness_p2_visa_value)) if fitness_p2_visa_value is not None else 0  # Convert to float and round
                fitness_p2_visa_value = f"{fitness_p2_visa_value:,}" if fitness_p2_visa_value is not None else 0
                fitness_p2_visa_value = str(fitness_p2_visa_value) if fitness_p2_visa_value is not None else 0  # Convert back to string

                fitness_p2_mc_value = round(float(fitness_p2_mc_value)) if fitness_p2_mc_value is not None else 0  # Convert to float and round
                fitness_p2_mc_value = f"{fitness_p2_mc_value:,}" if fitness_p2_mc_value is not None else 0
                fitness_p2_mc_value = str(fitness_p2_mc_value) if fitness_p2_mc_value is not None else 0  # Convert back to string

                ebook_p2_value = round(float(ebook_p2_value)) if ebook_p2_value is not None else 0  # Convert to float and round
                ebook_p2_value = f"{ebook_p2_value:,}" if ebook_p2_value is not None else 0
                ebook_p2_value = str(ebook_p2_value) if ebook_p2_value is not None else 0  # Convert back to string
                
                other_products_value = round(float(other_products_value)) if other_products_value is not None else 0  # Convert to float and round
                other_products_value = f"{other_products_value:,}" if other_products_value is not None else 0
                other_products_value = str(other_products_value) if other_products_value is not None else 0  # Convert back to string
                
                initials_vp1_count = round(float(initials_vp1_count)) if initials_vp1_count is not None else 0
                initials_vp1_count = f"{initials_vp1_count:,}" if initials_vp1_count is not None else 0
                initials_vp1_count = str(initials_vp1_count) if initials_vp1_count is not None else 0
                
                initials_mp1_count = round(float(initials_mp1_count)) if initials_mp1_count is not None else 0
                initials_mp1_count = f"{initials_mp1_count:,}" if initials_mp1_count is not None else 0
                initials_mp1_count = str(initials_mp1_count) if initials_mp1_count is not None else 0
                
                rebill_vp2_count = round(float(rebill_vp2_count)) if rebill_vp2_count is not None else 0
                rebill_vp2_count = f"{rebill_vp2_count:,}" if rebill_vp2_count is not None else 0
                rebill_vp2_count = str(rebill_vp2_count) if rebill_vp2_count is not None else 0
                
                rebill_mp2_count = round(float(rebill_mp2_count)) if rebill_mp2_count is not None else 0
                rebill_mp2_count = f"{rebill_mp2_count:,}" if rebill_mp2_count is not None else 0
                rebill_mp2_count = str(rebill_mp2_count) if rebill_mp2_count is not None else 0

                fitness_p2_visa_count = round(float(fitness_p2_visa_count)) if fitness_p2_visa_count is not None else 0
                fitness_p2_visa_count = f"{fitness_p2_visa_count:,}" if fitness_p2_visa_count is not None else 0
                fitness_p2_visa_count = str(fitness_p2_visa_count) if fitness_p2_visa_count is not None else 0

                fitness_p2_mc_count = round(float(fitness_p2_mc_count)) if fitness_p2_mc_count is not None else 0
                fitness_p2_mc_count = f"{fitness_p2_mc_count:,}" if fitness_p2_mc_count is not None else 0
                fitness_p2_mc_count = str(fitness_p2_mc_count) if fitness_p2_mc_count is not None else 0

                ebook_p2_count = round(float(ebook_p2_count)) if ebook_p2_count is not None else 0
                ebook_p2_count = f"{ebook_p2_count:,}" if ebook_p2_count is not None else 0
                ebook_p2_count = str(ebook_p2_count) if ebook_p2_count is not None else 0
                
                other_products_count = round(float(other_products_count)) if other_products_count is not None else 0
                other_products_count = f"{other_products_count:,}" if other_products_count is not None else 0
                other_products_count = str(other_products_count) if other_products_count is not None else 0
                
                # Create dictionary with all variables and their values
                data = {
                    "Metric": [
                        "Initials VP1 Count", "Initials VP1 Approval %", "Initials VP1 Value",
                        "Initials MP1 Count", "Initials MP1 Approval %", "Initials MP1 Value",
                        "Rebill VP2 Count", "Rebill VP2 Approval %", "Rebill VP2 Value",
                        "Rebill MP2 Count", "Rebill MP2 Approval %", "Rebill MP2 Value",
                        "Fitness P2 VISA Count", "Fitness P2 VISA Approval %", "Fitness P2 VISA Value",
                        "Fitness P2 MC Count", "Fitness P2 MC Approval %", "Fitness P2 MC Value",
                        "Ebook P2 Count", "Ebook P2 Approval %", "Ebook P2 Value",
                        "Other Products Count", "Other Products Approval %", "Other Products Value",
                        "Gross", "Net",
                        "Chargebacks Count", "Chargebacks", "Refund Count", "Refund", "refund_cs_count", "refund_cs_amount", "refund_alert_count", "refund_alert_amount"
                    ],
                    "Value": [
                        initials_vp1_count, initials_vp1_approval, initials_vp1_value,
                        initials_mp1_count, initials_mp1_approval, initials_mp1_value,
                        rebill_vp2_count, rebill_vp2_approval, rebill_vp2_value,
                        rebill_mp2_count, rebill_mp2_approval, rebill_mp2_value,
                        fitness_p2_visa_count, fitness_p2_visa_approval, fitness_p2_visa_value,
                        fitness_p2_mc_count, fitness_p2_mc_approval, fitness_p2_mc_value,
                        ebook_p2_count, ebook_p2_approval, ebook_p2_value, 
                        other_products_count, other_products_approval, other_products_value,
                        gross_revenue, net_revenue,
                        chargebacks_count, chargebacks, refund_count, refund, refund_cs_count, refund_cs_amount, refund_alert_count, refund_alert_amount
                    ]
                }

                # Convert to DataFrame
                df_metrics = pd.DataFrame(data)
                df_metrics['Value'] = df_metrics['Value'].replace('', np.nan).fillna(0)
                
                metrics = {
                    "db_id": client_id,  # Set db_id as the identifier in the metrics payload
                    "initials_vp1": initials_vp1_count,
                    "initials_vp1_revenue": initials_vp1_value,
                    "initials_vp1_approval": initials_vp1_approval,
                    
                    "initials_mp1": initials_mp1_count,
                    "initials_mp1_revenue": initials_mp1_value,
                    "initials_mp1_approval": initials_mp1_approval,
                    
                    "rebill_vp2": rebill_vp2_count,
                    "rebill_vp2_revenue": rebill_vp2_value,
                    "rebill_vp2_approval": rebill_vp2_approval,
                    
                    "rebill_mp2": rebill_mp2_count,
                    "rebill_mp2_revenue": rebill_mp2_value,
                    "rebill_mp2_approval": rebill_mp2_approval,

                    "fitness_p2_visa": fitness_p2_visa_count,
                    "fitness_p2_visa_revenue": fitness_p2_visa_value,
                    "fitness_p2_visa_approval": fitness_p2_visa_approval,

                    "fitness_p2_mc": fitness_p2_mc_count,
                    "fitness_p2_mc_revenue": fitness_p2_mc_value,
                    "fitness_p2_mc_approval": fitness_p2_mc_approval,

                    "ebook_p2": ebook_p2_count,
                    "ebook_p2_revenue": ebook_p2_value,
                    "ebook_p2_approval": ebook_p2_approval,
                    
                    "other_products": other_products_count,
                    "other_products_revenue": other_products_value,
                    "other_products_approval": other_products_approval,

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
