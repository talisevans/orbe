# import libraries
import pyodbc
import pprint
from datetime import datetime, timedelta
import sys
import pandas as pd
import requests
from base64 import b64encode
import json
import io
import os

# Import Google Cloud Storage client library
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq


# --- GCP Configuration ---
# This script is configured to load credentials directly from a JSON file.
# It expects 'gcp_service_acct.json' to be in the same directory as this script.

GCS_BUCKET_NAME = "orbe_shortcuts" # Your GCS bucket name
ORBE_SALON_ID = "nth_adl"  # Your ORBE salon ID

# Construct the full, absolute path to the service account file
script_dir = os.path.dirname(os.path.realpath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(script_dir, 'gcp_service_acct.json')


# Initialize the GCS client using the service account key file
try:
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
except FileNotFoundError:
    print(f"Error: The service account key file was not found at {SERVICE_ACCOUNT_FILE}")
    print(f"Please make sure 'gcp_service_acct.json' is in the same directory as the script.")
    sys.exit(1)
except Exception as e:
    print(f"Error initializing GCS client: {e}")
    sys.exit(1) # Exit if we can't connect to GCP

# get a pretty print variable for debugging
pp = pprint.PrettyPrinter(indent=4)

# Get the SQL server connection from the Point of Sale machine
try:
    sqlsvr = pyodbc.connect(r"Driver={SQL Server Native Client 11.0};"
                          r"Server=ORBE-NA\SHORTCUTSPOS;"
                          r"Database=ShortcutsPOS;"
                          r"Trusted_Connection=yes;")
    sqlsvrCursor = sqlsvr.cursor()
except Exception as e:
    print(f"Error connecting to the on-premise SQL Server: {e}")
    sys.exit(1)


#############
# Functions #
#############

# get data from the source table (delta load)
def getSourceTableDataDelta( sqlsvrCursor, syncPeriodDate, sourceSQLTable, dateField  ):
  """
  Fetches records from the source SQL Server table that have been modified
  since the syncPeriodDate.
  """
  # declare the sync period date as a string
  syncPeriodDateString = syncPeriodDate.strftime('%Y-%m-%d')

  # run the query
  sql = f"SELECT * FROM {sourceSQLTable} WHERE {dateField} > ?"
  sqlsvrCursor.execute( sql, syncPeriodDateString )

  # get the columns to turn the result into a dictionary
  columns = [column[0] for column in sqlsvrCursor.description]
  results = [dict(zip(columns, row)) for row in sqlsvrCursor]

  return results

# get all data from the source table (full load)
def getSourceTableDataFull( sqlsvrCursor, sourceSQLTable  ):
  """
  Fetches all data from a source SQL Server table.
  """
  # Get the right SQL statement
  sql = ""
  if sourceSQLTable == "dbo.giftCertificate":
        sql = f'SELECT CertificateID, "Transaction Date" AS transactionDate, "Transaction Number" AS transactionNumber, SundryID AS sundryId, Variance AS variance, Redeemed AS redeemed, ExpiryDate AS expiryDate, HistoryID AS historyId FROM {sourceSQLTable}'
  else:
        sql = f"SELECT * FROM {sourceSQLTable}"

  # get data from the sql server
  sqlsvrCursor.execute( sql )

  # get the columns to turn the result into a dictionary
  columns = [column[0] for column in sqlsvrCursor.description]
  results = [dict(zip(columns, row)) for row in sqlsvrCursor]

  return results

# function to process and upload data to GCS as a Parquet file
def processUploadData( bucket, destination_object_name, sourceData ):
    """
    Converts source data to a Pandas DataFrame, then to Parquet format,
    and uploads it to the specified GCS bucket.
    """
    if not sourceData:
        print(f"No data to upload for {destination_object_name}. Skipping.")
        return

    try:
        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(sourceData)

        # Coerce all data to string type to avoid parquet schema issues with mixed types
        # BigQuery can handle casting strings back to appropriate types later.
        df = df.astype(str)

        # Create an in-memory buffer
        buffer = io.BytesIO()

        # Write the DataFrame to the buffer in Parquet format
        df.to_parquet(buffer, index=False, engine='pyarrow')

        # Rewind the buffer to the beginning
        buffer.seek(0)

        # Create a blob object for the destination file in GCS
        # We'll name the file based on the table name and the sync date
        today_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        blob_name = f"data/{destination_object_name}/{ORBE_SALON_ID}/{today_str}.parquet"
        blob = bucket.blob(blob_name)

        # Upload the buffer's content to the blob
        blob.upload_from_file(buffer, content_type='application/octet-stream')

        print(f"Successfully uploaded {destination_object_name} to gs://{bucket.name}/{blob_name}")

    except Exception as e:
        print(f"An error occurred during data processing or upload for {destination_object_name}: {e}")

# declare the main function to sync tables by a transaction date
def syncTableDeltas( bucket, sqlsvrCursor, destinationObjectName, sourceSQLTable, dateField, syncPeriodDate ):
  """
  Coordinates fetching delta data and uploading it to GCS.
  """
  # get data from the source SQL server tables
  sourceData = getSourceTableDataDelta( sqlsvrCursor, syncPeriodDate, sourceSQLTable, dateField )

  # process and upload the data to GCS
  processUploadData( bucket, destinationObjectName, sourceData )

# declare the main function to sync full tables
def syncFullTable( bucket, sqlsvrCursor, destinationObjectName, sourceSQLTable ):
  """
  Coordinates fetching full table data and uploading it to GCS.
  """
  # get data from the source SQL server tables
  sourceData = getSourceTableDataFull( sqlsvrCursor, sourceSQLTable )

  # process and upload the data to GCS
  processUploadData( bucket, destinationObjectName, sourceData )

###########
# Runtime #
###########

# update Terminal
print('Starting data sync to Google Cloud Storage')

# work out the sync period. Always sync the past 800 days
daysLookback = 1095
syncPeriodDate = datetime.now() - timedelta(days=daysLookback)

# get Sales Transaction Lines
print('Starting Sale Transaction Lines')
syncTableDeltas( bucket, sqlsvrCursor, 'SaleTransactionLine', 'dbo.scvSaleTransactionLine', 'TransactionDate', syncPeriodDate )

# Get Appointment All
print('Starting Appointment All table')
syncTableDeltas( bucket, sqlsvrCursor, 'AppointmentAll', 'dbo.scvAppointmentAll', 'AppointmentDate', syncPeriodDate )

# # Get Sale Transactions
# print('Starting Sale Transactions')
# syncTableDeltas( bucket, sqlsvrCursor, 'SaleTransaction', 'dbo.scvSaleTransaction', 'TransactionDate', syncPeriodDate )

# # Get Appointments
# print('Starting Appointments table')
# syncTableDeltas( bucket, sqlsvrCursor, 'Appointment', 'dbo.scvAppointment', 'AppointmentDate', syncPeriodDate )

# # Get the Appointments Recurring table
# print('Starting Appointments recurring table')
# syncTableDeltas( bucket, sqlsvrCursor, 'AppointmentRecurAll', 'dbo.scvAppointmentRecurAll', 'AppointmentDate', syncPeriodDate )

# Get Clients
print('Starting Clients')
syncFullTable( bucket, sqlsvrCursor, 'Client', 'dbo.scvClient' )

# Get the Employee Site table
print('Starting Employee Site Table')
syncFullTable( bucket, sqlsvrCursor, 'EmployeeSite', 'dbo.scvEmployeeSite' )

# # Get Payments by Date Client table
# print('Starting Payments by Date Client table')
# syncTableDeltas( bucket, sqlsvrCursor, 'PaymentsByDateClient', 'dbo.scvPaymentsByDateClient', 'PaymentDate', syncPeriodDate )

# # Get Sale Product by Date table
# print('Starting Sale Product by Date')
# syncTableDeltas( bucket, sqlsvrCursor, 'SalesProductByDate', 'dbo.scvSalesProductByDate', 'SaleDate', syncPeriodDate )

# # get Sale Service by Date table
# print('Starting Sale Service by Date')
# syncTableDeltas( bucket, sqlsvrCursor, 'SalesServiceByDate', 'dbo.scvSalesServiceByDate', 'SaleDate', syncPeriodDate )

# # Get sale transaction line discount table
# print('Starting Sale Transaction Line Discount')
# syncFullTable( bucket, sqlsvrCursor, 'SaleTransactionLineDiscount', 'dbo.scvSaleTransactionLineDiscount' )

# # Get sale transaction payment table
# print('Starting Sale Transaction Payment')
# syncTableDeltas( bucket, sqlsvrCursor, 'SaleTransactionPayment', 'dbo.scvSaleTransactionPayment', 'TransactionDate', syncPeriodDate )

# Get Sale Service Site Report table
print('Starting Sale Service Site Report')
syncFullTable( bucket, sqlsvrCursor, 'ServiceSiteReportCategory', 'dbo.scvServiceSiteReportCategory' )

# # Get Gift Certificates table
# print('Gift Certificates')
# syncFullTable( bucket, sqlsvrCursor, 'giftCertificates', 'dbo.giftCertificate' )

# Job done
print('All Done!')
