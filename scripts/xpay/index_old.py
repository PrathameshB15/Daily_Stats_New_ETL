import psycopg2
import pandas as pd
import configparser
import requests
import configparser
import psycopg2
import pandas as pd
import requests
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
        img_width, img_height = 1100, 900  # Adjusted image size to fit all content
        img = Image.new('RGB', (img_width, img_height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)

            # Add gray background outside the border
        gray_color = (211, 211, 211)  # Light gray color
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
            logo.thumbnail((80, 80))  # Resize logo to fit (adjust as needed)
        except IOError:
            print("Logo file not found or could not be opened.")
            return

        # Load fonts (You can use custom fonts for better appearance)
        try:
            title_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 55)  # Larger title font size
            subtitle_font = ImageFont.truetype("extras/ttf/Segoe UI Bold.ttf", 30)  # Bold subtitle font
            header_font = ImageFont.truetype("extras/ttf/segoe-ui-semibold.ttf", 35)  # Larger font for headers
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
        logo_y =90  # Y position for both logo and title (increased space from top)
        title_y = logo_y + (logo.height) // 2  # Vertically center title with logo

        # Paste logo
        img.paste(logo, (start_x, logo_y), logo)


        # Get today's date and subtract 1 day
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime("%b %d, %Y")  # Format as "Nov 06, 2024"

        # Create the subtitle text with the current date minus one day
        subtitle_text = f"Top 5 Campaigns - Initial Performance: {formatted_date}"
        # Subtitle - "Daily Metrics" in bold and blue
        subtitle_bbox = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
        subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
        # midnight_blue = (25, 25, 112)   # Standard blue color (RGB)
        subtitle_y = logo_y + logo.height + 40  # Position the subtitle below the title
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
        header_y = subtitle_y + 90  # Position the header below the subtitle

        # Define row height (space between each metric row)
        row_height = 70  # Increase this value to create more space between metrics
        
        # Add metrics in tabular format
        y_position = header_y  # Starting position for the metrics

        # metrics_data = [
        #     ("Campaign 1 Name", "Campaign 1 Approvals","Campaign 1 Approval Rate", "Campaign 1 Revenue"),
        #     ("Campaign 2 Name", "Campaign 2 Approvals","Campaign 2 Approval Rate", "Campaign 2 Revenue"),
        #     ("Campaign 3 Name", "Campaign 3 Approvals","Campaign 3 Approval Rate", "Campaign 3 Revenue"),
        #     ("Campaign 4 Name", "Campaign 4 Approvals","Campaign 4 Approval Rate", "Campaign 4 Revenue"),
            
        # ]
        
        metrics_data = []

        for i in range(1, len(df) + 1):  # `df` contains your campaign rows
            metrics_data.append((
                f"Campaign {i} Name",
                f"Campaign {i} Approvals",
                f"Campaign {i} Approval Rate",
                f"Campaign {i} Revenue"
            ))

        # Track the y-position of the last two metrics (Gross and Net) for line placement
        gross_y_position = None
        net_y_position = None
        
        client_metrics_dict = dict(zip(client_metrics['Metric'], client_metrics['Value']))

        
        # Determine number of campaigns dynamically (each campaign has 4 metrics)
        campaign_count = len([k for k in client_metrics_dict if k.startswith("Campaign") and "Name" in k])

        # Initialize result list
        campaign_data_list = []

        # Loop through available campaigns
        for i in range(1, campaign_count + 1):
            name_key = f"Campaign {i} Name"
            count_key = f"Campaign {i} Approvals"
            approval_key = f"Campaign {i} Approval Rate"
            revenue_key = f"Campaign {i} Revenue"

            # Get values from the dict, defaulting to safe values
            campaign_name = client_metrics_dict.get(name_key, f"Unknown Campaign {i}")
            count_value = client_metrics_dict.get(count_key, "0") or "0"
            approval_value = client_metrics_dict.get(approval_key, "0") or "0"
            revenue_value = client_metrics_dict.get(revenue_key, "0") or "0"
                
            
                

            # Format the count and revenue values safely
            # count_text = f"{count_value:,}" if count_value else ""
            # approval_text = f"{approval_value}%" if approval_value else ""
            # # Ensure revenue_value is numeric before applying the format
            # revenue_text = f"${revenue_value:,.0f}" if revenue_value is not None else "$0"
            
            # count_text = f"{int(count_value):,}" if count_value and str(count_value).isdigit() else ""

            count_text = (
                f"{int(count_value.replace(',', '')):,}"
                if count_value and count_value.replace(",", "").isdigit()
                else ""
            )

            
            #  Remove '%' before converting to float
            if approval_value:
                approval_value_cleaned = approval_value.replace("%", "").strip()
                approval_text = f"{float(approval_value_cleaned):.1f}%" if approval_value_cleaned.replace(".", "").isdigit() else ""
            else:
                approval_text = ""
            
            # revenue_text = f"${float(revenue_value):,.0f}" if revenue_value is not None and str(revenue_value).replace(".", "").isdigit() else "$0"
            revenue_text = (
                f"${float(revenue_value.replace(',', '')):,.0f}"
                if revenue_value is not None and revenue_value.replace(",", "").replace(".", "").isdigit()
                else "$0"
            )
            

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
            
            count_x = table_start_x + header1_width - count_width + offset_all - 100 # Right-align count column
            metric_name_x = table_start_x + header1_width + count_metric_spacing + offset_all  # Left-align metric name column
            shift_right = 30  # move 30px to the right

            revenue_x = (
                table_start_x
                + header1_width
                + header2_width
                + 2 * metric_revenue_spacing
                + header3_width
                + offset_rev
                + offset_all
            )

        
            # Define a fixed position for the approval column
            approval_column_offset = header1_width + count_metric_spacing + (header2_width / 2)

            # Calculate the approval text width
            approval_width = approval_bbox[2] - approval_bbox[0]
            
            # Calculate approval_x based on a fixed offset
            approval_x = table_start_x + approval_column_offset + offset - approval_width + offset_all + 100


            # Set color only for the specified metric names
            name_color = (105, 105, 105) 
            name_font = regular_font  # Use bold font for "Gross" and "Net"
            
            revenue_color = (40, 165, 40)

                
            count_color = (0, 0, 205)
            approval_color = (0, 0, 205)
            
            # Draw the count, metric name, and revenue text
            # draw.text((count_x, y_position), count_text, font=regular_font, fill=count_color)  # Draw the count value
            # draw.text((metric_name_x, y_position), campaign_name, font=name_font, fill=name_color)  # Draw the metric name centered
            # draw.text((approval_x, y_position), approval_text, font=name_font, fill=approval_color)  # Draw the metric name centered
            # draw.text((revenue_x, y_position), revenue_text, font=regular_font, fill=revenue_color)  # Draw the revenue value
            
            
            # Set up wrapping
            max_line_length = 25  # adjust based on image width
            line_spacing = 5

            # Wrap the campaign name
            wrapped_name = wrap(campaign_name, width=max_line_length)

            # Get the height of a single line of text (using textbbox to calculate the size of the text)
            dummy_text = wrapped_name[0]
            bbox = draw.textbbox((0, 0), dummy_text, font=name_font)
            name_line_height = bbox[3] - bbox[1]  # height of the text from bounding box

            # Calculate total height taken by wrapped name
            wrapped_height = len(wrapped_name) * (name_line_height + line_spacing)

            # Draw the count, approval, and revenue at the same y_position (before updating y)
            draw.text((count_x, y_position), count_text, font=regular_font, fill=count_color)
            draw.text((approval_x, y_position), approval_text, font=name_font, fill=approval_color)
            draw.text((revenue_x, y_position), revenue_text, font=regular_font, fill=revenue_color)

            # Draw the wrapped campaign name, line by line
            for i, line in enumerate(wrapped_name):
                line_y = y_position + i * (name_line_height + line_spacing)
                draw.text((metric_name_x, line_y), line, font=name_font, fill=name_color)

            # Update y_position for the next row of metrics (move down by wrapped name height + spacing)
            y_position += wrapped_height + 10  # 10 is extra padding after the block

            # # Add division lines above Gross and Net metrics only
            # line_padding = 10  # Space above the metrics for the division line
            
            # # Division line above the Gross metric
            # line_y = y_position + line_padding
            # draw.line([(table_start_x, line_y), (table_start_x + total_table_width, line_y)], fill=(0, 0, 0), width=1)
            y_position += 20
            
            
    
        # Ensure the 'images' directory exists
        if not os.path.exists('images'):
            os.makedirs('images')

        # Save the image to the 'images' folder with the client_id as the filename
        image_filename = f"images/xpay_{client_id}.png"
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

    # Initialize an empty dictionary to store results by client
    client_results = {}

    # Initialize an empty list to store the final result
    result = []

    client_ids = [10008, 10009, 10010, 10011, 10012, 10013, 10014, 10016]
    # client_ids = [10011]
    
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
                            VAR TopCampaigns =
                                TOPN (
                                    5,
                                    ADDCOLUMNS (
                                        SUMMARIZE (
                                            FILTER (
                                                order_details,
                                                order_details[date_of_sale] >= TODAY() - 1 &&
                                                order_details[Sales Type] = "Initials" &&
                                                order_details[is_test] = "No" &&
                                                order_details[order_total] <> 0
                                            ),
                                            order_details[Campaign name]
                                        ),
                                        "AttemptCount", CALCULATE (
                                            [# Attempts],
                                            order_details[date_of_sale] >= TODAY() - 1,
                                            order_details[Sales Type] = "Initials",
                                            order_details[is_test] = "No",
                                            order_details[order_total] <> 0
                                        ),
                                        "Approvals", CALCULATE (
                                            [# Approvals],
                                            order_details[date_of_sale] >= TODAY() - 1,
                                            order_details[Sales Type] = "Initials",
                                            order_details[is_test] = "No",
                                            order_details[order_total] <> 0
                                        ),
                                        "Total", CALCULATE (
                                            SUM(order_details[order_total]),
                                            order_details[date_of_sale] >= TODAY() - 1,
                                            order_details[Sales Type] = "Initials",
                                            order_details[is_test] = "No",
                                            order_details[order_total] <> 0,
                                            order_details[order_status] = "Approved"
                                        )
                                    ),
                                    [AttemptCount], DESC
                                )
                            RETURN
                                SELECTCOLUMNS (
                                    TopCampaigns,
                                    "Campaign name", [Campaign name],
                                    "AttemptCount", [AttemptCount],
                                    "Approvals", [Approvals],
                                    "Total", [Total]
                                )
                            ORDER BY
                                [AttemptCount] DESC


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

                            # Create DataFrame directly from the rows
                            df = pd.DataFrame(rows)
                            
                            if df.empty:
                                break

                            # Optional: Rename columns to cleaner names
                            df.columns = [col.strip('[]') for col in df.columns]  # removes brackets if any
                            
                            # Keep only top 5 rows
                            df = df.head(5)
                            
                            # Replace NaN or blanks (empty strings) with 0
                            df = df.fillna(0).replace('', 0)

                            # Convert numeric columns to int if needed
                            df['AttemptCount'] = df['AttemptCount'].astype(int)
                            df['Approvals'] = df['Approvals'].astype(int)
                            df['Total'] = df['Total'].astype(int)
                            
                            # Calculate approval rate as a percentage
                            df['approval_rate'] = (df['Approvals'] / df['AttemptCount'].replace(0, 1)) * 100

                            # Optional: round approval rate to 2 decimal places
                            df['approval_rate'] = df['approval_rate'].round(1)

                            # df now contains Campaign name, AttemptCount, and Approvals
                            print(df)
                            
                            # Loop through each campaign and assign transformed values as string variables
                            for i in range(min(5, len(df))):
                                campaign = df.iloc[i]

                                # Extract values and transform
                                name = str(campaign['Campaign name'])
                                revenue = str(round(campaign['Total']))
                                approvals = str(round(campaign['Approvals']))
                                approval_rate = str(round(campaign['approval_rate'], 2))

                                # Assign to string variables
                                globals()[f'campaign{i+1}_name'] = name
                                globals()[f'campaign{i+1}_revenue'] = revenue
                                globals()[f'campaign{i+1}_approvals'] = approvals
                                globals()[f'campaign{i+1}_approval_rate'] = approval_rate

                            # Optional: Print the results
                            for i in range(min(5, len(df))):
                                print(f"Campaign {i+1} -> Name: {globals()[f'campaign{i+1}_name']}, "
                                    f"Revenue: {globals()[f'campaign{i+1}_revenue']}, "
                                    f"Approvals: {globals()[f'campaign{i+1}_approvals']}, "
                                    f"Approval Rate: {globals()[f'campaign{i+1}_approval_rate']}%")
                                
                            campaign_data = {
                                "Metric": [],
                                "Value": []
                            }

                            for i in range(1, len(df) + 1):  # Loop only over available rows
                                name_var = globals().get(f'campaign{i}_name', 'N/A')
                                revenue_var = globals().get(f'campaign{i}_revenue', '0')
                                approvals_var = globals().get(f'campaign{i}_approvals', '0')
                                rate_var = globals().get(f'campaign{i}_approval_rate', '0')

                                campaign_data["Metric"].extend([
                                    f"Campaign {i} Name", 
                                    f"Campaign {i} Revenue", 
                                    f"Campaign {i} Approvals", 
                                    f"Campaign {i} Approval Rate"
                                ])
                                
                                campaign_data["Value"].extend([
                                    name_var,
                                    revenue_var,
                                    approvals_var,
                                    rate_var
                                ])
                                
                            # Add client_id and last update timestamp
                            campaign_data["Metric"].extend(["client_id", "last_daily_update_xpay"])
                            campaign_data["Value"].extend([str(client_id), str(int(datetime.now().timestamp()))])
                            
                            current_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                            file_name = "3x Icon.png"
                            logo = os.path.join(current_folder, file_name)
                            
                            image_file = generate_and_save_image(campaign_data, client_id, logo)

                            # Add the new date metric to the existing metrics dictionary
                            campaign_data["yesterday_date"] = (datetime.now() - timedelta(days=1)).strftime('%b %d, %Y')
                            
                            # Convert to a dictionary for easier access
                            data_dict = dict(zip(campaign_data['Metric'], campaign_data['Value']))
                            
                            # Extract campaign data only
                            metrics = {}
                            campaign_idx = 1

                            while f"Campaign {campaign_idx} Name" in data_dict:
                                metrics[f"campaign_{campaign_idx}_name"] = data_dict.get(f"Campaign {campaign_idx} Name")
                                metrics[f"campaign_{campaign_idx}_revenue"] = float(data_dict.get(f"Campaign {campaign_idx} Revenue", 0))
                                metrics[f"campaign_{campaign_idx}_approvals"] = int(data_dict.get(f"Campaign {campaign_idx} Approvals", 0))
                                metrics[f"campaign_{campaign_idx}_approval_rate"] = float(data_dict.get(f"Campaign {campaign_idx} Approval Rate", 0))
                                campaign_idx += 1

                            # metrics["Metric"].extend(["client_id", "last_daily_update_xpay"])
                            # metrics["Value"].extend([str(client_id), str(int(datetime.now().timestamp()))])
                            # # Add the new date metric to the existing metrics dictionary
                            # metrics["yesterday_date"] = (datetime.now() - timedelta(days=1)).strftime('%b %d, %Y')
                            
                            # Step 3: Add client_id and last_daily_update_xpay
                            metrics["client_id"] = data_dict.get("client_id")
                            metrics["last_daily_update_xpay"] = int(datetime.now().timestamp())

                            # Step 4: Add yesterday's date
                            metrics["yesterday_date"] = (datetime.now() - timedelta(days=1)).strftime('%b %d, %Y')
                            
                            # Final result
                            from pprint import pprint
                            pprint(metrics)
                            
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




                        except requests.exceptions.RequestException as e:
                            print(f"API request failed for page_id {page_id}: {e}")
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
