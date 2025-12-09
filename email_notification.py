import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import configparser

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

def send_email(subject, body, attachment_path=None):
    # Email configuration
    sender_email = config.get('production', 'sender_email')
    sender_password = config.get('production', 'sender_password')
    # recipient_email = config.get('production', 'recipient_email')

     # Fetch and split recipient emails from config.ini
    recipient_email_str = config.get('production', 'recipient_email')
    recipient_emails = [email.strip() for email in recipient_email_str.split(',')]  # Convert to list
    
    # Create a multipart message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(recipient_emails)  # For display purposes in the email header
    message['Subject'] = subject

    # Add body to email
    message.attach(MIMEText(body, 'plain'))
    
    # Attach the file if provided
    if attachment_path:
        with open(attachment_path, 'rb') as attachment:
            # MIMEBase is used for binary data
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={attachment_path}')
            message.attach(part)

    # Connect to SMTP server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # Login to email server
    server.login(sender_email, sender_password)

    # Send email
    server.sendmail(sender_email, recipient_emails, message.as_string())

    # Quit server
    server.quit()