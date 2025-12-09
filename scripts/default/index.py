import configparser
import os
import psycopg2
import pandas as pd
import requests
import sys
import argparse
from decimal import Decimal
import numpy as np
# Add the root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from execution_summary import summary
from email_notification import send_email
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
import time

# Load the main config.ini
config = configparser.ConfigParser()
config.read("config.ini")

script_name = os.path.basename(os.path.dirname(__file__))

# Check if the script should run
if config['General'].getboolean(f'run_{script_name}'):
    print(f"✅ Running {script_name}...")
    # Your existing script logic here
    
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

    # Read database and Customer.io configuration from config.ini
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Customer.io API settings
    CUSTOMER_IO_API_KEY = config.get("customer_io", "API_KEY")
    site_id = config.get("customer_io", "site_id")
    CUSTOMER_IO_BASE_URL = 'https://track.customer.io/api/v1/customers/'

    #Google drive folder id
    drive_id = config.get("production", "drive_id")

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
        
    # Function to convert Decimal values to float in the payload
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


    def generate_and_save_image(client_metrics, client_id, logo_path):
        # Increase the image size to accommodate the larger padding
        img_width, img_height = 900, 1100  # Adjusted image size to fit all content
        img = Image.new('RGB', (img_width, img_height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)

            # Add gray background outside the border
        gray_color = (210, 235, 255)
        # gray_color = (225, 240, 255)

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
        logo_y = 60  # Y position for both logo and title (increased space from top)
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
            ("Initials", "initials","initials_approval", "initials_revenue"),
            ("Rebills", "rebills","rebills_approval", "rebills_revenue"),
            ("Straight sales", "straight_sales","ss_approval", "straight_sales_revenue"),
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
            if metric_name == "Net" or metric_name == "Gross":
                count_text = f"{count_value:,}" if count_value else ""
                approval_text = f"{approval_value}%" if approval_value else ""
            else:
                count_text = f"{count_value:,}" if count_value else "0"
                approval_text = f"{approval_value}%" if approval_value else "0"
                
            # Ensure revenue_value is numeric before applying the format
            revenue_text = f"${revenue_value:,.0f}" if revenue_value is not None else "$0"
            
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

        
            # Define a fixed position for the approval column
            approval_column_offset = header1_width + count_metric_spacing + (header2_width / 2)

            # Calculate the approval text width
            approval_width = approval_bbox[2] - approval_bbox[0]
            
            # Calculate approval_x based on a fixed offset
            approval_x = table_start_x + approval_column_offset + offset - approval_width + offset_all


            # Set color only for the specified metric names
            name_color = (105, 105, 105) if metric_name in ["Initials", "Rebills", "Straight sales", "Chargebacks", "CS Refunds", "Alert Refunds"] else (0, 0, 0)
            name_font = subtitle_font if metric_name in ["Gross", "Net"] else regular_font  # Use bold font for "Gross" and "Net"
            
            revenue_color = None
            if metric_name in ["Initials", "Rebills", "Straight sales", "Gross", "Net"]:
                revenue_color = (40, 165, 40)  # Lime Green
            elif metric_name in ["Chargebacks", "CS Refunds", "Alert Refunds"]:
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
                y_position += 120
            elif metric_name == "Straight sales" or metric_name == "Alert Refunds":
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


    # Function to push metrics to Customer.io
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

    # Fetch Refund Alert metrics from Power BI for a given client (special-case client 10042)
    def fetch_refund_alert_metrics(client_id):
        """Fetch refund alert count and amount from Power BI for a specific client.
        Returns a dict with keys 'refund_alert_count' and 'refund_alert_amount' or None on failure.
        """
        try:
            # Get report id for the client
            report_query = f"select id from beast_insights_v2.reports where client_id = {client_id} and is_active = 'True' and is_deleted = 'False' limit 1;"
            reports_df = fetch_data_from_db(report_query)
            if reports_df is None or reports_df.empty:
                return None

            for report_id in reports_df['id']:
                page_query = f"SELECT id FROM beast_insights_v2.pages WHERE report_id = {report_id} and is_active = 'True' and is_deleted = 'False' limit 1;"
                page_df = fetch_data_from_db(page_query)
                if page_df is None or page_df.empty:
                    continue

                for page_id in page_df['id']:
                    # Login and get cookies
                    cookies = None
                    try:
                        cookies = requests.post('https://bi-api.beastinsights.co/api/user/login', json={"usernameOrEmail": f"dev_{client_id}", "password": "Dev@beastinsights/#2024"}).cookies.get_dict()
                    except Exception:
                        cookies = None

                    if not cookies:
                        continue

                    url = f"https://bi-api.beastinsights.co/api/page/executeQueries/{page_id}"
                    headers = {
                        'Content-Type': 'application/json',
                        'cookie': '; '.join([f'{key}={value}' for key, value in cookies.items()])
                    }

                    # DAX query provided by user for refund alerts (uses fixed date as example)
                    payload = {
                        "query": """
                        EVALUATE
                        CALCULATETABLE(
                          SUMMARIZECOLUMNS(
                            'Calendar'[Date],
                            "# Refund Alert", [## Refund by (Refund Date)_alert],
                            "$ Refund Alert", [$$ Refund by (Refund Date)_alert]
                          ),
                          order_details[is_test] = "No",
                          KEEPFILTERS( FILTER( order_alerts, order_alerts[is_tc40] <> "Yes" ) ),
                          NOT( ISBLANK( order_details[order_total] ) ),
                          'Calendar'[Date] = TODAY() - 1
                        )
                        """
                    }

                    max_retries = 3
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            resp = requests.post(url, headers=headers, json=payload, timeout=120)
                            resp.raise_for_status()
                            data = resp.json()

                            rows = data.get('data', {}).get('results', [])[0].get('tables', [])[0].get('rows', [])
                            if not rows:
                                return None

                            flat_list = [list(d.values()) for d in rows][0]

                            # Expecting two values per row; sum across rows if multiple dates returned
                            total_count = 0
                            total_amount = 0
                            for item in flat_list:
                                # If the result is a scalar (single row with both columns won't appear as flat_list), handle defensively
                                pass

                            # If rows contain dicts with keys mapping to the two columns, extract per-row
                            # But common response here gives rows as [{'# Refund Alert': X, '$ Refund Alert': Y}, ...]
                            # So rebuild totals properly
                            totals = {'count': 0, 'amount': 0}

                            for r in rows:
                                if isinstance(r, dict):
                                    cnt = r.get('[# Refund Alert]', 0)
                                    amt = r.get('[$ Refund Alert]', 0)

                                    try:
                                        totals['count'] += int(float(cnt))
                                    except:
                                        pass

                                    try:
                                        totals['amount'] += float(amt)
                                    except:
                                        pass

                            return {
                                'refund_alert_count': totals['count'],
                                'refund_alert_amount': totals['amount']
                            }

                        except requests.exceptions.RequestException as e:
                            attempt += 1
                            time.sleep(5)
                            continue

            return None
        except Exception:
            return None

    # Function to get active clients from beast_insights_v2.clients table filtered by CRM
    def get_active_clients(client_ids=None):
        """Fetch list of active clients from beast_insights_v2.clients table filtered by CRM ID"""
        try:
            # If specific client IDs are provided, return them directly
            if client_ids:
                print(f"Using provided client IDs: {client_ids}")
                return [str(client_id) for client_id in client_ids]
            # Query to get active clients from the clients table filtered by CRM credentials
            # clients_query = """
            # SELECT c.id 
            # FROM beast_insights_v2.clients c
            # INNER JOIN beast_insights_v2.crm_credentials cc ON c.id = cc.client_id
            # WHERE c.is_active = True 
            # AND c.is_deleted = False
            # AND cc.crm_id = 1
            # AND c.id NOT IN (10027, 10000, 10005, 10030, 10001)
            # ORDER BY c.id ASC;
            # """

            clients_query = """
            SELECT c.id 
            FROM beast_insights_v2.clients c
            INNER JOIN beast_insights_v2.crm_credentials cc ON c.id = cc.client_id
            WHERE c.is_active = True 
            AND c.is_deleted = False
            AND cc.crm_id = 1
            AND c.id IN (10043, 10044, 10045, 10046, 10047, 10048, 10049, 10050, 10051, 10052, 10053, 10054, 10055, 10056)
            ORDER BY c.id ASC;
            """
            clients_df = fetch_data_from_db(clients_query)
            
            if clients_df is not None and not clients_df.empty:
                # Extract client IDs and convert to strings for consistency
                client_ids = [str(client_id) for client_id in clients_df['id'].tolist()]
                
                print(f"Found {len(client_ids)} active clients for CRM ID 1: {client_ids}")
                return client_ids
            
            return []
        except Exception as e:
            print(f"Error fetching active clients: {e}")
            return []

    def parse_arguments():
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description='Daily Stats ETL Script - Default')
        parser.add_argument('--client-ids', 
                          nargs='+',
                          type=int,
                          help='List of client IDs to process (e.g., --client-ids 10007 10008)')
        return parser.parse_args()

    # Parse command line arguments
    args = parse_arguments()
    
    try:
        # Get list of active clients dynamically or use provided client IDs
        active_clients = get_active_clients(args.client_ids if args.client_ids else None)
        
        if not active_clients:
            print("No active clients found. Exiting.")
            exit()
        
        print(f"Processing {len(active_clients)} clients serially: {active_clients}")
        
        # Get yesterday's date for querying
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Fetching data for date: {yesterday}")
        
        # Process each client individually
        for client_id in active_clients:
            print(f"\nProcessing client {client_id}...")
            
            # Build query for individual client
            client_query = f"""
            SELECT
              {client_id} AS client_id,
              -- counts & revenue by sales type (names match your Python keys)
              COALESCE(SUM(CASE WHEN sales_type = 'Initials' THEN approvals END), 0) AS initials,
              COALESCE(SUM(CASE WHEN sales_type = 'Initials' THEN revenue END), 0) AS initials_revenue,

              COALESCE(SUM(CASE WHEN sales_type = 'Rebills' THEN approvals END), 0) AS rebills,
              COALESCE(SUM(CASE WHEN sales_type = 'Rebills' THEN revenue END), 0) AS rebills_revenue,

              COALESCE(SUM(CASE WHEN sales_type = 'Straight Sales' THEN approvals END), 0) AS straight_sales,
              COALESCE(SUM(CASE WHEN sales_type = 'Straight Sales' THEN revenue END), 0) AS straight_sales_revenue,

              -- gross revenue (total)
              COALESCE(SUM(revenue), 0) AS gross_revenue,

              -- chargebacks
              COALESCE(SUM(cb_cb_date), 0) AS chargebacks_count,
              COALESCE(SUM(cb_cb_date_dollar), 0) AS chargebacks,

              -- refund splits (Alert / CS)
              COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date END), 0) AS refund_alert_count,
              COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date_dollar END), 0) AS refund_alert_amount,

              COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date END), 0) AS refund_cs_count,
              COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date_dollar END), 0) AS refund_cs_amount,

              -- approval rates (safe division)
              ( COALESCE(SUM(CASE WHEN sales_type = 'Initials' THEN approvals_organic END),0)
                / NULLIF(COALESCE(SUM(CASE WHEN sales_type = 'Initials' THEN attempts END),0), 0)
              ) AS initials_approval,

              ( COALESCE(SUM(CASE WHEN sales_type = 'Rebills' THEN approvals_organic END),0)
                / NULLIF(COALESCE(SUM(CASE WHEN sales_type = 'Rebills' THEN attempts END),0), 0)
              ) AS rebills_approval,

              ( COALESCE(SUM(CASE WHEN sales_type = 'Straight Sales' THEN approvals_organic END),0)
                / NULLIF(COALESCE(SUM(CASE WHEN sales_type = 'Straight Sales' THEN attempts END),0), 0)
              ) AS ss_approval,

              -- net revenue calculation (gross - chargebacks - refund_alert_amount - refund_cs_amount)
              (
                COALESCE(SUM(revenue),0)
                - COALESCE(SUM(cb_cb_date_dollar),0)
                - COALESCE(SUM(CASE WHEN refund_type = 'Refund Alert' THEN refund_refund_date_dollar END),0)
                - COALESCE(SUM(CASE WHEN refund_type = 'Refund CS' THEN refund_refund_date_dollar END),0)
              ) AS net_revenue

            FROM "reporting"."order_summary_{client_id}"
            WHERE date = '{yesterday}';
            """
            
            try:
                # Execute query for this specific client
                metrics_df = fetch_data_from_db(client_query)

                if metrics_df is not None and not metrics_df.empty:
                    metrics_df = metrics_df.fillna(0)
                    
                    # Process the single row result for this client
                    row = metrics_df.iloc[0]  # Get first (and only) row
                    db_id = client_id  # Use the client_id directly
                    
                    print(f"Processing metrics for client {db_id}")

                    # If client is 10042, fetch refund alert metrics from Power BI
                    refund_alert_count_val = row.get('refund_alert_count', 0)
                    refund_alert_amount_val = row.get('refund_alert_amount', 0)
                    if str(db_id) == '10042':
                        pbi_vals = fetch_refund_alert_metrics(db_id)
                        if pbi_vals:
                            refund_alert_count_val = pbi_vals.get('refund_alert_count', refund_alert_count_val)
                            refund_alert_amount_val = pbi_vals.get('refund_alert_amount', refund_alert_amount_val)

                    # Format approval rates from the query (already calculated as decimals)
                    initials_approval = (
                        f"{row['initials_approval'] * 100:.1f}"
                        if row['initials_approval'] is not None and row['initials_approval'] != 0
                        else "0.0"
                    )

                    rebills_approval = (
                        f"{row['rebills_approval'] * 100:.1f}"
                        if row['rebills_approval'] is not None and row['rebills_approval'] != 0
                        else "0.0"
                    )

                    ss_approval = (
                        f"{row['ss_approval'] * 100:.1f}"
                        if row['ss_approval'] is not None and row['ss_approval'] != 0
                        else "0.0"
                    )
                    
                    metrics = {
                        "db_id": db_id,  # Set db_id as the identifier in the metrics payload
                        "initials": row['initials'],
                        "initials_revenue": row['initials_revenue'],
                        "rebills": row['rebills'],
                        "rebills_revenue": row['rebills_revenue'],
                        "straight_sales": row['straight_sales'],
                        "straight_sales_revenue": row['straight_sales_revenue'],
                        "gross_revenue": row['gross_revenue'],
                        "chargebacks_count": row['chargebacks_count'],
                        "chargebacks": row['chargebacks'],
                        "net_revenue": row['net_revenue'],
                        "refund_alert_count": refund_alert_count_val,
                        "refund_alert_amount": refund_alert_amount_val,
                        "refund_cs_count": row['refund_cs_count'],
                        "refund_cs_amount": row['refund_cs_amount'],
                        "initials_approval": initials_approval,
                        "rebills_approval": rebills_approval,
                        "ss_approval": ss_approval,
                        "last_daily_update": int(datetime.now().timestamp())
                    }
                    
                    current_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                    file_name = "3x Icon.png"
                    logo = os.path.join(current_folder, file_name)
                    image_file = generate_and_save_image(metrics, db_id, logo)
                    
                    # Add the new date metric to the existing metrics dictionary
                    metrics["yesterday_date"] = (datetime.now() - timedelta(days=1)).strftime('%b %d, %Y')

                    # Format numeric values with commas
                    for key, value in metrics.items():
                        if isinstance(value, (Decimal, float)) and key not in ["initials_approval", "rebills_approval", "ss_approval"]:
                            metrics[key] = f"{int(value):,}"

                    print(f"Generated metrics for client {db_id}: {metrics}")
                    push_to_customer_io(db_id, metrics)
                    
                else:
                    print(f"No data found for client {client_id}")
                    
            except Exception as e:
                print(f"Error processing client {client_id}: {str(e)}")
                summary.add_failure(
                    f"Daily Metrics refresh failed for client {client_id}", 
                    f"Daily Metrics refresh failed for client {client_id} : {str(e)}"
                )   
            
        # Specify the Google Drive folder ID if you have a specific folder
        # Leave as None if you want to upload to the root folder
        google_drive_folder_id = drive_id # Replace with your Google Drive folder ID or set to None
        CLIENT_ID = config.get("production", "imgur_client_id")
        # upload_images_to_imgur()
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
        summary.add_failure(
            f"Daily Metrics refresh failed for client {db_id}", 
            f"Daily Metrics refresh failed for client {db_id} : {str(e)}"
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
