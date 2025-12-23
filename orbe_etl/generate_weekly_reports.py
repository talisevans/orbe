"""
Example script to generate weekly performance reports.

This script demonstrates how to use the WeeklyReportGenerator class
to create Excel reports with weekly performance data.
"""

from src.gcp_bucket import GCPBucket
from src.etl import Orbe_ETL
from src.weekly_report import WeeklyReportGenerator
from datetime import datetime
import pandas as pd
from google.cloud import secretmanager
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
from pathlib import Path


################
## Functions ##
##############

# Get AWS Secret
def get_secret(project_id, secret_id, version_id="latest"):
    """
    Retrieve a secret from GCP Secret Manager.

    Args:
        project_id: GCP project ID
        secret_id: The ID of the secret to retrieve
        version_id: The version of the secret (default: "latest")

    Returns:
        The secret value as a string
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Email report function
def email_report(recipient_emails, subject, body, attachment_path):
    """
    Send an email with an Excel attachment using AWS SES SMTP.

    Args:
        recipient_emails: List of recipient email addresses
        subject: Email subject line
        body: Email body text
        attachment_path: Path to the Excel file to attach
    """
    # Get AWS SES credentials from GCP Secret Manager
    secret_string = get_secret("talis-evans", "aws_ses")
    aws_credentials = json.loads(secret_string)
    SMTP_host = aws_credentials['host']
    SMTP_username = aws_credentials['username']
    SMTP_password = aws_credentials['password']
    SMTP_port = int(aws_credentials['port'])

    # Sender email with name
    sender_name = "Talis Evans"
    sender_email = "no-reply@talisevans.dev"

    # Create message container
    msg = MIMEMultipart()
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = ', '.join(recipient_emails)
    msg['Subject'] = subject

    # Add body to email
    msg.attach(MIMEText(body, 'plain'))

    # Attach the Excel file if it exists
    attachment_file = Path(attachment_path)
    if attachment_file.exists():
        with open(attachment_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_file.name)
            msg.attach(attachment)
    else:
        print(f"Warning: Attachment file not found at {attachment_path}")

    # Send email via AWS SES SMTP
    try:
        with smtplib.SMTP(SMTP_host, SMTP_port) as server:
            server.starttls()  # Secure the connection
            server.login(SMTP_username, SMTP_password)
            server.send_message(msg)

        print(f"Email successfully sent to {', '.join(recipient_emails)}")
        print(f"Subject: {subject}")
        print(f"Attachment: {attachment_file.name}")

    except Exception as e:
        print(f"Error sending email: {str(e)}")
        raise


#############
## Runtime ##
#############

# Initialize GCP Bucket
GCPBucket = GCPBucket("orbe_shortcuts", "australia-southeast2")

# declare proper location name lookups
location_name_mappings = {
    "nth_adl": "Orbe North Adelaide",
    "norwood": "Orbe Norwood"
}

# loop through the different locations
for location in ["nth_adl","norwood"]:

    # proper name lookup
    location_name = location_name_mappings[location]

    # Update Terminal
    print(f'Generating report for {location_name}... ')

    """
        Uses the following Parquet files from the GCP Bucket:
        - AppointmentAll
        - SaleTransactionLine
        - Client
        - EmployeeSite
        - ServiceSiteReportCategory
    """

    # Get data from GCP Bucket
    print('Fetching data from GCP Bucket...')
    appointments_df = GCPBucket.getAppointments(location)
    sales_transactions_df = GCPBucket.getSalesTransactions(location)
    productive_hours_df = GCPBucket.getProductiveHours(location)
    clients_df = GCPBucket.load_latest_parquet("Client", location)
    staff_df = GCPBucket.load_latest_parquet("EmployeeSite", location)
    reporting_categories = GCPBucket.load_latest_parquet("ServiceSiteReportCategory", location)

    # Create the Orbe ETL object
    print('Building datasets...')
    orbe_etl = Orbe_ETL(
        appointments_df=appointments_df,
        sales_transactions_df=sales_transactions_df,
        productive_hours_df=productive_hours_df,
        clients_df=clients_df,
        staff_df=staff_df,
        reporting_categories=reporting_categories
    )

    # Build datasets
    detail_df = orbe_etl.build_detail_dataset()
    rebooking_df = orbe_etl.build_rebooking_dataset()

    # Create the weekly report generator
    print('Generating weekly reports...')

    # Determine logo path (container vs local)
    logo_path = 'img/orbe-logo.png' if Path('img/orbe-logo.png').exists() else 'orbe_etl/img/orbe-logo.png'

    report_gen = WeeklyReportGenerator(
        detail_df=detail_df,
        rebooking_df=rebooking_df,
        productive_hours_df=productive_hours_df,
        clients_df=clients_df,
        staff_df=staff_df,
        logo_path=logo_path,
        store_name=location_name
    )

    # Generate all weeks up to last Sunday
    report_gen.generate_all_weeks()

    # Save the workbook
    output_filename = f'{location_name}_weekly_performance_report_{datetime.now().strftime("%Y%m%d")}.xlsx'
    report_gen.save('tmp/' + output_filename)

    # email the report
    print('Emailing the report...')
    email_report(
        recipient_emails=['joe@orbe.com.au', 'talis.evans@gmail.com'],
        subject=f'Weekly Performance Report - {location_name}', 
        body=f'Please find attached the latest weekly performance report for {location_name}.',
        attachment_path='tmp/' + output_filename
    )

print("All Done!")
