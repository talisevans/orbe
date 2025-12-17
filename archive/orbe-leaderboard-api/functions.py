
# need to install pip3 install mysql-connector-python
from pydoc import cli
import mysql.connector
from mysql.connector import Error
from flask import session, request
from flask_session import Session
import string
from datetime import datetime, timedelta, date
import random
import string
from variables import *
import time
import pandas as pd
from pandas.io import sql

########################
## Database Functions ##
########################

# function to establish MySQL connection
def get_mysql():

    # connect to MySQL
    try:
        connection = mysql.connector.connect(host='206.189.150.30',
                                            database='Shortcuts',
                                            user='ONA',
                                            password='aqCGp?wW2c*Xz9V-',
                                            autocommit=True)  # auto commits query data
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

    except Error as e:
        print("Error while connecting to MySQL", e)

    return connection;


# generate secutiry token
def generate_token(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


###############
## Settings ##
##############

# returns a dictionary of staff settings
def dbQueryGetStaffSettings( mysql_cursor ):

    # declare the response list
    responseList = [ ];

    # get staff for settings
    mysql_cursor.execute("""
        SELECT
            EmployeeSite.EmployeeId,
            CONCAT( FirstName, ' ', Surname ) AS employeeName,
            targetIncome,
            targetRebookings,
            showEmployee
        FROM EmployeeSite
        LEFT JOIN EmployeeTargets
        ON EmployeeSite.EmployeeId = EmployeeTargets.EmployeeId
        WHERE IsActive = 1
        ORDER BY CONCAT( FirstName, ' ', Surname )
    """)
    rows = mysql_cursor.fetchall()

    # build the response list
    for staffRow in rows:

        # declare the tmp row variable
        tmpRow = {};
        tmpRow['EmployeeId'] = staffRow.get('EmployeeId')
        tmpRow['employeeName'] = staffRow.get('employeeName').strip()
        tmpRow['targetIncome'] = 0
        tmpRow['targetRebookings'] = 0
        tmpRow['showEmployee'] = False

        # only add target income if it exists
        if staffRow.get('targetIncome') is not None:
            tmpRow['targetIncome'] = staffRow.get('targetIncome')

        # only add target rebooking if it exists
        if staffRow.get('targetRebookings') is not None:
            tmpRow['targetRebookings'] = staffRow.get('targetRebookings')

        # if the string is 'true', then update the show employees variable
        if staffRow.get('showEmployee') == 'true':
            tmpRow['showEmployee'] = True

        # append this row to the response
        responseList.append(tmpRow)

    return responseList

# db query used to return a lookup disctionary of staff names
def dbStaffNamesLookup( mysql_cursor ):

    # declare the response list
    responseList = {};

    # get staff for settings
    mysql_cursor.execute("""
        SELECT
            EmployeeSite.EmployeeId,
            CONCAT( FirstName, ' ', Surname ) AS employeeName
        FROM EmployeeSite
    """)
    rows = mysql_cursor.fetchall()

    # build the response list
    for staffRow in rows:

        # declare the tmp row variable
        responseList['staff-'+str(staffRow.get('EmployeeId'))] = staffRow.get('employeeName').strip()

    return responseList

#####################
## Auth Functions ##
####################

@login.verify_password
def verify_password(username, password):

    # chill for 5 seconds.  Helps prevent bruit force attacks
    # time.sleep(5)  # not needed for development.  Bring it back in for production

    # then do the username validation check
    if username in users and \
            check_password_hash(users.get(username), password):
        return username
        
@auth.verify_token
def verify_token(token):

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor( dictionary=True )

    # get token information
    mysql_cursor.execute("SELECT token, expiry, ipAddress FROM leaderboardAPI WHERE token = (%s)", [ token ])
    rows = mysql_cursor.fetchall()

    # get the user's IP address
    ipAddress = request.remote_addr

    # get the token record (if one exists)
    for tokenRecord in rows:

        #  if this token hasn't expired
        if int(tokenRecord['expiry'] ) > int(datetime.now().timestamp()):

            # then check to make sure the token and ip address match.  If they do, login the user
            if token == tokenRecord['token'] and ipAddress == tokenRecord['ipAddress']:
                return True
    

############################
## Leaderboard Functions ##
###########################

# gets actual revenue and rebookings by staff
def dbGetStaffActuals( mysql_cursor, leaderboardPrepETL, weekStart, weekEnd ):

    ########################
    # Get Actual Earnings #
    #######################

    # start by getting a list of sales transac``tions that have occurred
    mysql_cursor.execute("""
        SELECT
            EmployeeId,
            SUM(LineIncTaxAmount) AS LineIncTaxAmount
        FROM SaleTransactionLine
        WHERE DATE(TransactionDate) BETWEEN DATE(%s) AND DATE(%s) 
        AND ItemTypeStringCode = "ItemType.Service"  # show only services
        GROUP BY EmployeeId
    """, [ weekStart.strftime("%Y-%m-%d"), weekEnd.strftime("%Y-%m-%d") ])
    rows = mysql_cursor.fetchall()

    # loop through each row
    for staffRevenue in rows:

        # if this staff member exists in the leaderboard 
        if str(staffRevenue['EmployeeId']) in leaderboardPrepETL:

            # then add the actual data for this staff member
            leaderboardPrepETL[str(staffRevenue['EmployeeId'])]['revenue']['actual'] = round(staffRevenue['LineIncTaxAmount'],0)


    ##########################
    # Get Actual Rebookings #
    ########################

    # start by getting a list of sales transactions that have occurred
    mysql_cursor.execute("""
        SELECT
            COUNT( DISTINCT CONCAT(clientId,'---',visitDate) ) AS numVisits,
            SUM(isRebooked) AS numRebookings,
            employeeId
        FROM rebookingOutput
        WHERE DATE(visitDate) BETWEEN DATE(%s) AND DATE(%s)
        GROUP BY employeeId
    """, [ weekStart.strftime("%Y-%m-%d"), weekEnd.strftime("%Y-%m-%d") ])
    rows = mysql_cursor.fetchall()

    # loop through each row
    for staffRebooking in rows:

        # if this staff member exists in the leaderboard 
        if str(staffRebooking['employeeId']) in leaderboardPrepETL:

            # # calculate the rebooking percentage
            rebookingPercentage = float(round( (staffRebooking['numRebookings'] / staffRebooking['numVisits']),2 ))

            # # then add the actual data for this staff member
            leaderboardPrepETL[str(staffRebooking['employeeId'])]['rebooking']['actual'] = rebookingPercentage

    
    ###############################
    # Get Actual Retail Products #
    ##############################

    # start by getting a list of sales transac``tions that have occurred
    mysql_cursor.execute("""
        SELECT
            EmployeeId,
            SUM(LineIncTaxAmount) AS productSales,
            SUM(ItemQuantity) AS unitSales
        FROM SaleTransactionLine
        WHERE DATE(TransactionDate) BETWEEN DATE(%s) AND DATE(%s)
        AND ItemTypeStringCode = "ItemType.Product"  # show only products
        GROUP BY EmployeeId
    """, [ weekStart.strftime("%Y-%m-%d"), weekEnd.strftime("%Y-%m-%d") ])
    rows = mysql_cursor.fetchall()

    # loop through each row
    for staffRevenue in rows:

        # if this staff member exists in the leaderboard 
        if str(staffRevenue['EmployeeId']) in leaderboardPrepETL:

            # then add the actual data for this staff member
            leaderboardPrepETL[str(staffRevenue['EmployeeId'])]['products']['revenue'] = round(staffRevenue['productSales'],0)
            leaderboardPrepETL[str(staffRevenue['EmployeeId'])]['products']['units'] = round(staffRevenue['unitSales'],0)

    
    return leaderboardPrepETL;
    


###########################
# Resync Rebooking Table #
#########################

def resyncRebookingTable( mysql_cursor ):

    #resync rebookings for the past 10 days
    startDate = datetime.today() - timedelta(days=10)
    endDate = datetime.today()

    # rebookingPrepDictionary
    rebookingDict = {}

    #################################
    # Step 1: Get Transaction Data #
    ################################

    # start by getting a list of sales transactions that have occurred
    mysql_cursor.execute("""
        SELECT
            EmployeeId,
            ClientId,
            DATE(TransactionDate) AS TransactionDate,
            ItemTypeStringCode,
            SUM(LineIncTaxAmount) AS LineIncTaxAmount
        FROM SaleTransactionLine
        WHERE DATE(TransactionDate) BETWEEN DATE(%s) AND DATE(%s) 
        GROUP BY EmployeeId, ClientId, DATE(TransactionDate), ItemTypeStringCode
    """, [ startDate.strftime("%Y-%m-%d"), endDate.strftime("%Y-%m-%d") ])
    rows = mysql_cursor.fetchall()

    # loop through all transactions coming back
    for staffResult in rows:

        # create a unique client visit variable
        uniqueClientVisit = staffResult['TransactionDate'].strftime("%Y-%m-%d") + '---' + str(staffResult['ClientId'])

        # declare the dictionary for this clientId if it doesn't already exist
        if staffResult['ClientId'] not in rebookingDict:
            rebookingDict[staffResult['ClientId']] = {}

        # declare the dictionary for this unique client visit if it doesn't already exist
        if uniqueClientVisit not in rebookingDict[staffResult['ClientId']]:
            rebookingDict[staffResult['ClientId']][uniqueClientVisit] = {}

        # declare the dictionary for this employeeId if it doesn't already exist
        if staffResult['EmployeeId'] not in rebookingDict[staffResult['ClientId']][uniqueClientVisit]:
            rebookingDict[staffResult['ClientId']][uniqueClientVisit][staffResult['EmployeeId']] = {}

        # declare the dictionary for this item type string code
        if staffResult['ItemTypeStringCode'] not in rebookingDict[staffResult['ClientId']][uniqueClientVisit][staffResult['EmployeeId']]:
            rebookingDict[staffResult['ClientId']][uniqueClientVisit][staffResult['EmployeeId']][staffResult['ItemTypeStringCode']] = 0

        # increment the staff value for this sale transaction
        rebookingDict[staffResult['ClientId']][uniqueClientVisit][staffResult['EmployeeId']][staffResult['ItemTypeStringCode']] += staffResult['LineIncTaxAmount'];


    ###########################
    # Step 2: ETL Processing #
    ##########################

    # declare the rebooking ETL
    rebookingETL = {}

    # loop through each client
    for clientId in rebookingDict:

        # exclude walk in clients from the rebooking calculation
        if clientId == 1:  # clientId 1 is #WALK IN
            continue

        # loop through each uniqueVisit
        for uniqueVisit in rebookingDict[clientId]:

            # we only include visits in the rebooking calculation if they have a service type attached
            hasServiceType = False;

            # visit Date
            visitDate = uniqueVisit[0:10]

            # declare variables to calculate which staffId should get this booking (based on value of servicese provided)
            calcStaffId = -1;
            maxValue  = 0;
            totalBookingValue = 0;

            # loop through each staff Id
            for staffId in rebookingDict[clientId][uniqueVisit]:

                # declare a variable to track the value of products / services provided by this staff member
                loopStaffIdVal = 0;

                # loop through each item type
                for itemType in rebookingDict[clientId][uniqueVisit][staffId]:

                    # if we have a service attached to this visit, then include it in the rebooking calculation
                    if itemType == 'ItemType.Service':
                        hasServiceType = True;

                    # count the value of this service by employee
                    loopStaffIdVal += rebookingDict[clientId][uniqueVisit][staffId][itemType]
                    
                # add this staff member's value to the total booking value
                totalBookingValue += loopStaffIdVal
                
                # if this staffId has the most value of the booking so far
                if loopStaffIdVal > maxValue:

                    # then update the max value
                    maxValue = loopStaffIdVal

                    # and assign the booking to this staff id
                    calcStaffId = staffId

            # if this unique visit has a service attached to it
            if hasServiceType == True:

                # declare the list for this clientId if it doesn't already exist
                if clientId not in rebookingETL:
                    rebookingETL[clientId] = []

                # add this value to the rebooking ETL
                rebookingETL[clientId].append({
                    'EmployeeId': calcStaffId,
                    'visitDate': visitDate,
                    'bookingValue': round(totalBookingValue,2),
                    'isRebooked': False
                })

            
    ################################
    # Step 3: Get Appointment Data #
    ################################

    # start by getting a list of all future appointments by clientId
    mysql_cursor.execute("""
        SELECT
            CustomerId,
            MAX(DATE(AppointmentDate)) AS appointmentDate
        FROM AppointmentAll
        WHERE IsDeletedAppointment != 1
        AND IsCancellation != 1
        AND IsNoShow != 1
        AND DATE(AppointmentDate) > DATE(%s)
        GROUP BY CustomerId
    """, [ startDate.strftime("%Y-%m-%d") ])
    rows = mysql_cursor.fetchall()

    # loop through all appointments coming back
    for appointment in rows:

        # if this customerId is in the rebooking ETL
        if appointment['CustomerId'] in rebookingETL:

            # loop through each unique visit
            for index, uniqueVisit in enumerate(rebookingETL[ appointment['CustomerId']]):

                # get the visit date 
                visitDate = datetime.strptime(uniqueVisit['visitDate'], "%Y-%m-%d")

                # if the max appointment date for this client is after the visit date
                if appointment['appointmentDate'] > visitDate.date():

                    # then say the client is rebooked
                    rebookingETL[appointment['CustomerId']][index]['isRebooked'] = True;


    ###################################################
    # Step 4: Convert ETL Dictionary into basic list #
    ##################################################

    # declare the rebooking output list
    rebookingOutput = []

    # loop through all indexed clients and visits
    for clientId in rebookingETL:
        for visit in rebookingETL[clientId]:

            # make sure there is a charge for this booking
            if visit['bookingValue'] > 0:

                # add to the rebooking output
                rebookingOutput.append({
                    'visitDate': visit['visitDate'],
                    'clientId': clientId,
                    'employeeId': visit['EmployeeId'],
                    'bookingValue': visit['bookingValue'],
                    'isRebooked': visit['isRebooked'],
                    'dataDate': datetime.today().strftime("%Y-%m-%d")
                })


    ########################################################
    # Step 5: Delete Existing records for this date range #
    ######################################################

    # start by getting a list of sales transactions that have occurred
    mysql_cursor.execute("""DELETE FROM rebookingOutput WHERE DATE(visitDate) BETWEEN DATE(%s) AND DATE(%s)""", 
    [ startDate.strftime("%Y-%m-%d"), endDate.strftime("%Y-%m-%d") ])


    ############################
    # Step 6: Upload New Data #
    ##########################

    # declare pandas dataframe
    df=pd.DataFrame(rebookingOutput)

    # write the dataframe to SQL
    df.to_sql(con=engine, name='rebookingOutput', index=False, if_exists='append', chunksize=1000 )
