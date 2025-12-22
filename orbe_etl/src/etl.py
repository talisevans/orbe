"""
 Main ETL class to build the Detail Dataset

"""

import pandas as pd

class Orbe_ETL:

    appointments_df = None
    sales_transactions_df = None
    productive_hours_df = None
    clients_df = None
    staff_df = None
    reporting_categories = None

    def __init__(self, appointments_df, sales_transactions_df,
                 productive_hours_df, clients_df, staff_df, reporting_categories):
        self.appointments_df = appointments_df
        self.sales_transactions_df = sales_transactions_df
        self.productive_hours_df = productive_hours_df
        self.clients_df = clients_df
        self.staff_df = staff_df
        self.reporting_categories = reporting_categories

    # build the detailed dataset
    def build_detail_dataset(self):

        # declare a new detail df
        detail_df = pd.DataFrame()

        # From the sales_transactions_df, build the detail dataset
        detail_df = self.sales_transactions_df.copy()

        # Convert TransactionDate to date (not datetime) and rename to Date
        detail_df['Date'] = pd.to_datetime(detail_df['TransactionDate']).dt.date

        # Create Sale Product Quantity column
        detail_df['Sale Product Quantity'] = detail_df.apply(
            lambda row: row['ItemQuantity'] if row['ItemTypeStringCode'] == 'ItemType.Product' else None,
            axis=1
        )

        # Group by dimensions and sum the monetary columns
        detail_df = detail_df.groupby([
            'Date',
            'EmployeeId',
            'ClientId',
            'ItemTypeStringCode',
            'ItemId',
            'ItemName'
        ], as_index=False, dropna=False).agg({
            'LineIncTaxAmount': 'sum',
            'LineExTaxAmount': 'sum',
            'TaxAmount': 'sum',
            'Sale Product Quantity': 'sum',
            'ItemQuantity': 'sum'
        })

        # Lookup client information from clients_df
        detail_df = detail_df.merge(
            self.clients_df[['ClientId', 'FirstName', 'Surname']],
            on='ClientId',
            how='left'
        )

        # Create Client Name column by combining FirstName and Surname
        detail_df['Client Name'] = detail_df['FirstName'] + ' ' + detail_df['Surname']

        # Drop the intermediate columns
        detail_df = detail_df.drop(columns=['FirstName', 'Surname'])

        # add isNewClient column
        detail_df = self.add_is_new_client_column(detail_df)

        # add item reporting category - either a LOOKUP for items or 'Product' for products
        detail_df['ReportCategory'] = detail_df.apply(
            lambda row: self.get_item_report_category(row['ItemId'])
                       if row['ItemTypeStringCode'] == 'ItemType.Service'  # lookup the item if it's an item
                       else 'Product' if row['ItemTypeStringCode'] == 'ItemType.Product' # set to 'Product' if it's a product
                       else 'Prepayment' if row['ItemTypeStringCode'] == 'ItemType.BookingPrepayment' # set to 'Other' if it's a prepayment
                       else None,
            axis=1
        )

        # add staff name lookup
        detail_df = detail_df.merge(
            self.staff_df[['EmployeeId', 'FirstName', 'Surname']],
            on='EmployeeId',
            how='left'
        )

        # create Staff Name column
        detail_df['Staff Name'] = detail_df['FirstName'] + ' ' + detail_df['Surname']

        # Drop the intermediate columns
        detail_df = detail_df.drop(columns=['FirstName', 'Surname'])

        return detail_df

    # build the productive hours dataset
    def build_productive_hours_dataset(self):
        """
        Build a dataset with productive hours for each employee by date.

        Returns:
            DataFrame with AppointmentDate, EmployeeId, and DurationMinutes
        """
        # If appointments_df is None, return None
        if self.appointments_df is None:
            return None

        # Prepare the appointments dataset
        appointments_df = self.appointments_df.copy()

        # Filter appointments where IsArrived is True and sum DurationMinutes by date and employee
        productive_hours = appointments_df[appointments_df['IsArrived'] == True].groupby(
            ['AppointmentDate', 'EmployeeId'],
            as_index=False
        ).agg({
            'DurationMinutes': 'sum'
        })

        # Rename AppointmentDate to Date for consistency
        productive_hours = productive_hours.rename(columns={'AppointmentDate': 'Date'})

        # Convert Date to date format
        productive_hours['Date'] = pd.to_datetime(productive_hours['Date']).dt.date

        return productive_hours

    # build the rebooking dataset
    def build_rebooking_dataset(self):
        """
        Build a dataset to track client rebooking behavior.

        Returns a dataframe with columns: AppointmentDate, CustomerId, EmployeeId,
        is_new_client, is_rebooked.

        For appointments with multiple staff, the employee is determined by finding
        which employee had the highest spend for that client on that date.

        Returns:
            DataFrame with rebooking data, or None if appointments_df is None
        """
        # If appointments_df is None, return None
        if self.appointments_df is None:
            return None

        # Prepare all appointments data (for checking future bookings)
        all_appointments = self.appointments_df.copy()
        all_appointments['AppointmentDate'] = pd.to_datetime(all_appointments['AppointmentDate']).dt.date

        # Filter appointments where IsArrived is True (for checking prior appointments)
        arrived_appointments = self.appointments_df.copy()

        # set IsArrived to an integer
        arrived_appointments['IsArrived'] = arrived_appointments['IsArrived'].astype(int)

        # keep only appointments where IsArrived is 1
        arrived_appointments = arrived_appointments[arrived_appointments['IsArrived'] == 1]

        # Convert AppointmentDate to date
        arrived_appointments['AppointmentDate'] = pd.to_datetime(arrived_appointments['AppointmentDate']).dt.date

        # Get unique AppointmentDate + CustomerId combinations
        unique_appointments = arrived_appointments[['AppointmentDate', 'CustomerId']].drop_duplicates()

        """
            There can be walk-ins and others clients that don't necessarily have an appointment
            So, we look at the sales transactions to get unique client-date visits that might not have been captured in the appointment book
        
        """

        # get unique sales transactions
        unique_sales_transactions = self.sales_transactions_df[['TransactionDate', 'ClientId']].drop_duplicates()

        # add any unique sales transactions that are not in unique_appointments
        # Prepare sales transactions with date conversion
        sales_for_merge = unique_sales_transactions.copy()
        sales_for_merge['TransactionDate'] = pd.to_datetime(sales_for_merge['TransactionDate']).dt.date

        # Rename columns to match appointments structure
        sales_for_merge = sales_for_merge.rename(columns={
            'TransactionDate': 'AppointmentDate',
            'ClientId': 'CustomerId'
        })

        # Use merge with indicator to find transactions not in appointments
        merged = sales_for_merge.merge(
            unique_appointments,
            on=['AppointmentDate', 'CustomerId'],
            how='left',
            indicator=True
        )

        # Keep only transactions that don't have matching appointments
        new_transactions = merged[merged['_merge'] == 'left_only'][['AppointmentDate', 'CustomerId']]

        # Concatenate with existing appointments
        unique_appointments = pd.concat([unique_appointments, new_transactions], ignore_index=True)

        # reset the index of unique_appointments
        unique_appointments = unique_appointments.reset_index(drop=True)

        # Prepare sales transactions for matching
        sales_prep = self.sales_transactions_df.copy()
        sales_prep['TransactionDate'] = pd.to_datetime(sales_prep['TransactionDate']).dt.date

        # For each unique appointment, find the employee with highest spend
        def find_employee_with_highest_spend(row):
            appointment_date = row['AppointmentDate']
            customer_id = row['CustomerId']

            # Get all sales for this customer on this date
            matching_sales = sales_prep[
                (sales_prep['TransactionDate'] == appointment_date) &
                (sales_prep['ClientId'] == customer_id)
            ]

            if len(matching_sales) == 0:
                return None

            # Group by EmployeeId and sum the spend (using LineIncTaxAmount)
            employee_spend = matching_sales.groupby('EmployeeId')['LineIncTaxAmount'].sum()

            # Return the EmployeeId with the highest spend
            return employee_spend.idxmax()

        unique_appointments['EmployeeId'] = unique_appointments.apply(find_employee_with_highest_spend, axis=1)

        # Remove rows where we couldn't find an employee (no matching sales)
        rebooking_df = unique_appointments[unique_appointments['EmployeeId'].notna()].copy()

        # Add is_new_client column
        def check_new_client(row):
            customer_id = row['CustomerId']
            current_date = row['AppointmentDate']

            # Get all appointments for this client before this date where IsArrived is True
            prior_appointments = arrived_appointments[
                (arrived_appointments['CustomerId'] == customer_id) &
                (arrived_appointments['AppointmentDate'] < current_date)
            ]

            # Client is new if they have no prior appointments
            return len(prior_appointments) == 0

        rebooking_df['is_new_client'] = rebooking_df.apply(check_new_client, axis=1)

        # Add is_rebooked column
        def check_rebooked(row):
            customer_id = row['CustomerId']
            current_date = row['AppointmentDate']

            # Get all appointments for this client after this date (including future bookings)
            # Don't filter by IsArrived since future appointments haven't happened yet
            future_appointments = all_appointments[
                (all_appointments['CustomerId'] == customer_id) &
                (all_appointments['AppointmentDate'] > current_date)
            ]

            # Client is rebooked if they have future appointments
            return len(future_appointments) > 0

        rebooking_df['is_rebooked'] = rebooking_df.apply(check_rebooked, axis=1)

        return rebooking_df


    ######################
    ## Helper Functions ##
    ######################
    
    # add is new client column
    def add_is_new_client_column(self, detail_df):
        """
        Add an 'isNewClient' column to the detail dataset.

        For each row, determines if the ClientId had any prior appointments
        before the Date in that row. Returns True if no prior appointments exist.
        Only counts appointments where IsArrived is True.

        Args:
            detail_df: DataFrame with 'ClientId' and 'Date' columns

        Returns:
            DataFrame with added 'isNewClient' column (bool or None if no appointments data)
        """
        if self.appointments_df is None:
            detail_df['isNewClient'] = None
            return detail_df

        # Prepare appointments data - only include appointments where client arrived
        appointments_prep = self.appointments_df[
            self.appointments_df['IsArrived'] == True
        ][['AppointmentDate', 'CustomerId']].copy()
        appointments_prep['AppointmentDate'] = pd.to_datetime(appointments_prep['AppointmentDate']).dt.date

        # For each row in detail_df, check if there are any prior appointments
        def check_new_client(row):
            client_id = row['ClientId']
            current_date = row['Date']

            # Get all appointments for this client before this date where they arrived
            prior_appointments = appointments_prep[
                (appointments_prep['CustomerId'] == client_id) &
                (appointments_prep['AppointmentDate'] < current_date)
            ]

            # Client is new if they have no prior appointments
            return len(prior_appointments) == 0

        detail_df['isNewClient'] = detail_df.apply(check_new_client, axis=1)

        return detail_df

    def get_item_report_category(self, item_id):
        """
        Get the report category for an item based on its ID.

        Looks up the ItemId in the reporting_categories DataFrame (matched by ServiceId)
        and returns the first applicable report category name by checking
        IsReportCategory1 through IsReportCategory10 in order.

        Args:
            item_id: The ItemId to look up

        Returns:
            The report category name (str) if found, None otherwise
        """
        if self.reporting_categories is None:
            return None
        
        # Find the row matching this ItemId (using ServiceId as the key)
        matching_row = self.reporting_categories[
            self.reporting_categories['ServiceId'] == item_id
        ]

        if matching_row.empty:
            return None

        # Make a copy to avoid SettingWithCopyWarning
        row = matching_row.iloc[0].copy()

        # Check each report category in order (1-10)
        for i in range(1, 11):
            is_category_col = f'IsReportCategory{i}'
            category_name_col = f'ReportCategory{i}Name'

            # declare is_category_col as integer
            row[is_category_col] = int(row[is_category_col])

            if row[is_category_col] == 1:
                return row[category_name_col]

        return None




    

        