""""
GCP Bucket related functions.

"""
from google.cloud import storage
import pandas as pd
from typing import Optional
import os


class GCPBucket:
    """Class representing a GCP Bucket."""

    # declare a list of directories
    directories = [ 
        "Appointment", 
        "AppointmentAll", 
        "AppointmentRecurAll", 
        "Client", 
        "EmployeeSite", 
        "PaymentsByDateClient", 
        "SaleTransaction",
        "SaleTransactionLine",
        "SaleTransactionLineDiscount",
        "SaleTransactionPayment",
        "SalesProductByDate",
        "SalesServiceByDate",
        "ServiceSiteReportCategory",
        "giftCertificates",
        "ServiceSiteReportCategories"
    ]

    def __init__(self, name: str, location: str):
        self.name = name
        self.location = location

    # Method to get basic info about the bucket
    def get_info(self) -> str:
        """Return basic information about the bucket."""
        return f"GCP Bucket Name: {self.name}, Location: {self.location}"

    # Method to load the latest parquet file from a specific directory/store
    def load_latest_parquet(self, directory: str, store: str) -> Optional[pd.DataFrame]:
        """
        Download and load the most recent .parquet file from a specific directory/store path.

        Deletes all older .parquet files in the same location.

        Args:
            directory: The directory name (must be in self.directories list)
            store: The store name

        Returns:
            A pandas DataFrame containing the data from the most recent .parquet file,
            or None if the directory is not valid or no .parquet files are found.
        """
        # Check if directory is in the valid directories list
        if directory not in self.directories:
            print(f"Directory '{directory}' is not in the valid directories list.")
            return None

        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(self.name)

        # Construct the path prefix
        prefix = f"data/{directory}/{store}/"

        # List all blobs with the given prefix
        blobs = list(bucket.list_blobs(prefix=prefix))

        # Filter for .parquet files only
        parquet_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]

        if not parquet_blobs:
            print(f"No .parquet files found at {self.name}/{prefix}")
            return None

        # Sort by time_created to find the most recent file
        parquet_blobs.sort(key=lambda x: x.time_created, reverse=True)
        most_recent_blob = parquet_blobs[0]
        older_blobs = parquet_blobs[1:]

        # Download the most recent file to a temporary location
        temp_filename = f"/tmp/{os.path.basename(most_recent_blob.name)}"
        most_recent_blob.download_to_filename(temp_filename)
        print(f"Downloaded most recent file: {most_recent_blob.name}")

        # Load the parquet file to a DataFrame
        df = pd.read_parquet(temp_filename)

        # Clean up the temporary file
        os.remove(temp_filename)

        # Delete all older .parquet files
        for old_blob in older_blobs:
            old_blob.delete()
            print(f"Deleted old file: {old_blob.name}")

        return df

    def getAppointments(self, store: str) -> Optional[pd.DataFrame]:
        """
        Load and filter appointments for a specific store.

        Filters out deleted appointments, cancellations, and no-shows,
        then returns unique rows by AppointmentId, AppointmentDate, CustomerId, and EmployeeId.

        Args:
            store: The store name

        Returns:
            A pandas DataFrame containing filtered and deduplicated appointment data,
            or None if data cannot be loaded.
        """
        # Load the AppointmentAll data for the specified store
        df = self.load_latest_parquet("AppointmentAll", store)

        if df is None:
            return None
        
        # make sure isDeletedAppointment, IsCancellation, IsNoShow are integers
        df['IsDeletedAppointment'] = df['IsDeletedAppointment'].astype(int)
        df['IsCancellation'] = df['IsCancellation'].astype(int)
        df['IsNoShow'] = df['IsNoShow'].astype(int)

        # Filter out deleted appointments, cancellations, and no-shows
        appointments = df[
            (df['IsDeletedAppointment'] != 1) &
            (df['IsCancellation'] != 1) &
            (df['IsNoShow'] != 1)
        ]

        # Select required columns and remove duplicates
        appointments = appointments[['AppointmentId', 'AppointmentDate', 'CustomerId', 'EmployeeId', 'IsArrived']].drop_duplicates()

        # set AppointmentDate to a date (ignore time)
        appointments['AppointmentDate'] = pd.to_datetime(appointments['AppointmentDate']).dt.date

        # Add is_new_client column
        appointments = self.add_is_new_client_column(appointments)

        return appointments

    def getSalesTransactions(self, store: str) -> Optional[pd.DataFrame]:
        """
        Load and process sales transaction line data for a specific store.

        Selects specific fields and converts numeric fields to floats and date field to date only.

        Args:
            store: The store name

        Returns:
            A pandas DataFrame containing processed sales transaction data,
            or None if data cannot be loaded.
        """
        # Load the SaleTransactionLine data for the specified store
        df = self.load_latest_parquet("SaleTransactionLine", store)

        if df is None:
            return None

        # Select required columns
        sales = df[[
            'TransactionDate',
            'TransactionNumber',
            'TransactionLineId',
            'SaleNumber',
            'EmployeeId',
            'ClientId',
            'TransactionTypeStringCode',
            'ItemTypeStringCode',
            'ItemId',
            'ItemName',
            'ItemQuantity',
            'LineIncTaxAmount',
            'LineExTaxAmount',
            'TaxAmount'
        ]].copy()

        # Convert TransactionDate to date only (remove time component)
        sales['TransactionDate'] = pd.to_datetime(sales['TransactionDate']).dt.date

        # Convert numeric fields to floats
        numeric_fields = ['ItemQuantity', 'LineIncTaxAmount', 'LineExTaxAmount', 'TaxAmount']
        for field in numeric_fields:
            sales[field] = sales[field].astype(float)

        return sales

    def getProductiveHours(self, store: str) -> Optional[pd.DataFrame]:
        """
        Load appointments and sales data, then match them to create a productive hours dataset.

        Matches appointments with sales transactions based on date, customer/client, and service/item.
        Returns a dataframe with appointment details and associated item information for productivity tracking.

        Args:
            store: The store name

        Returns:
            A pandas DataFrame containing matched productive hours data with fields:
            AppointmentDate, CustomerId, EmployeeId, DurationMinutes, ItemId, ItemName
            or None if data cannot be loaded.
        """
        # Load the AppointmentAll data for the specified store
        appointments_df = self.load_latest_parquet("AppointmentAll", store)

        if appointments_df is None:
            return None

        # Load the SaleTransactionLine data
        sales_df = self.load_latest_parquet("SaleTransactionLine", store)

        if sales_df is None:
            return None
        
        # make sure isDeletedAppointment, IsCancellation, IsNoShow are integers
        appointments_df['IsDeletedAppointment'] = appointments_df['IsDeletedAppointment'].astype(int)
        appointments_df['IsCancellation'] = appointments_df['IsCancellation'].astype(int)
        appointments_df['IsNoShow'] = appointments_df['IsNoShow'].astype(int)

        # Filter out deleted appointments, cancellations, and no-shows
        appointments = appointments_df[
            (appointments_df['IsDeletedAppointment'] != 1) &
            (appointments_df['IsCancellation'] != 1) &
            (appointments_df['IsNoShow'] != 1)
        ]

        # Select and prepare appointment fields
        appointments = appointments_df[[
            'AppointmentDate',
            'CustomerId',
            'EmployeeId',
            'ServiceId',
            'DurationMinutes'
        ]].copy()

        # Convert AppointmentDate to date only
        appointments['AppointmentDate'] = pd.to_datetime(appointments['AppointmentDate']).dt.date

        # Select and prepare sales fields
        sales = sales_df[[
            'TransactionDate',
            'ClientId',
            'ItemId',
            'ItemName'
        ]].copy()

        # Convert TransactionDate to date only
        sales['TransactionDate'] = pd.to_datetime(sales['TransactionDate']).dt.date

        # Merge appointments with sales on matching criteria:
        # AppointmentDate = TransactionDate, CustomerId = ClientId, ServiceId = ItemId
        productive_hours = appointments.merge(
            sales,
            left_on=['AppointmentDate', 'CustomerId', 'ServiceId'],
            right_on=['TransactionDate', 'ClientId', 'ItemId'],
            how='inner'
        )

        # Select final columns for the productive hours dataframe
        productive_hours = productive_hours[[
            'AppointmentDate',
            'CustomerId',
            'EmployeeId',
            'DurationMinutes',
            'ItemId',
            'ItemName'
        ]]

        return productive_hours

    def add_is_new_client_column(self, appointments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add an 'is_new_client' column to the appointments dataset.

        For each row, determines if the CustomerId had any prior appointments
        before the AppointmentDate in that row. Returns True if no prior appointments exist.

        Args:
            appointments_df: DataFrame with 'CustomerId' and 'AppointmentDate' columns

        Returns:
            DataFrame with added 'is_new_client' column (bool)
        """
        # Create a copy to avoid modifying the original
        appointments = appointments_df.copy()

        # For each row in appointments, check if there are any prior appointments
        def check_new_client(row):
            customer_id = row['CustomerId']
            current_date = row['AppointmentDate']

            # Get all appointments for this customer before this date
            prior_appointments = appointments[
                (appointments['CustomerId'] == customer_id) &
                (appointments['AppointmentDate'] < current_date)
            ]

            # Client is new if they have no prior appointments
            return len(prior_appointments) == 0

        appointments['is_new_client'] = appointments.apply(check_new_client, axis=1)

        return appointments