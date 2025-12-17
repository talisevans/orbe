
#####################################################################
#  This is the API to interact with Xero for Orbe North Adelaide    #
#  It is run on a cronjob on my main Digital Ocean instance         #
#####################################################################

# import scripts
from asyncio import constants
import mysql.connector
import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import base64
import pprint
import time
from pymongo import MongoClient
from bson.objectid import ObjectId


# Declare app details
clientId = "4264A126B7044D1BB39E5D50CB6C8C5A"
clientSecret = "IKezZwYvk6bvi3ZApS1Vmsikf2Vo5c0HsFypPWqeIlmO6YAr"
tenantId = "2011fa15-b564-48f0-bdf5-fecce6fedc98"  # Orbe Nth Adl TenantId
tenantName = "Orbe North Adelaide"  # Tenant Name

# define pretty printer for debugging
pp = pprint.PrettyPrinter(indent=4)

# declare a variable to track the token last refresh time
tokenLastRefreshTime = None;
accessToken = ""

# by default, look back 10 days
lookbackDays = 10;

# connect to mongoDb in Digital Ocean
digitalOceanMongo = MongoClient('206.189.150.30',username='talis.evans',password='tes02029~~~')

# declare the mongodb database and collection to start the job
dbmonitor = digitalOceanMongo.osservare
durataCollection = dbmonitor['durata']
erroreCollection = dbmonitor['errore']



##############
# Functions #
############

# creates the MySQL database connection
def mysqlConnect( ):

    # connect to MySQL
    try:
        connection = mysql.connector.connect(host='206.189.150.30',
                                            database='ONA_Xero',
                                            user='ONA',
                                            password='aqCGp?wW2c*Xz9V-',
                                            auth_plugin='mysql_native_password',
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

        # send the error in for logging 
        # mydict = { "nome": "BPUC_MYOB", "ora": datetime.now(), "messaggio": "Error connecting to MySQL" + e }
        # erroreCollection.insert_one( mydict )



    return connection;

# function to refresh the access token if needed
def checkTimeGetAccessToken( ):

    # global access token
    global accessToken
    global tokenLastRefreshTime

    # get the current time
    now = datetime.datetime.now()

    # determine if we need to refresh
    needsRefresh = False
    if tokenLastRefreshTime == None:  # if we don't yet have a refresh token
        needsRefresh = True
    elif (now - tokenLastRefreshTime).total_seconds() > 900:  # 15 minutes * 60 seconds, or if 15 minutes have expired
        needsRefresh = True;

    # If we need to refresh
    if needsRefresh:

        # Update terminal
        print("We need to refresh our Access Token")

        # get the existing refresh token
        refreshToken = ""
        mycursor.execute("SELECT * FROM integration")
        myresult = mycursor.fetchall()
        for x in myresult:
            refreshToken = x['refreshToken']

        # Update Temrinal
        print('Existing refresh token collected');

        # Make a webservice call to get the updated response token
        url = "https://identity.xero.com/connect/token"
        payload={'grant_type': 'refresh_token', 'refresh_token': refreshToken,'client_id': clientId,}
        authString = clientId + ":" + clientSecret
        encoded_u = base64.b64encode(authString.encode()).decode()
        headers = {
            "Authorization": "Basic %s" % encoded_u
        }

        # read the response to dictionary
        response = requests.request("POST", url, headers=headers, data=payload)
        text = response.text
        responseObj = json.loads(text) 

        # update the refresh token and access token
        refreshToken = responseObj['refresh_token']
        accessToken = responseObj['access_token']

        # delete the existing refresh token in MySQL and insert the new one
        mycursor.execute("DELETE FROM integration")
        mycursor.execute("INSERT INTO integration ( refreshToken ) VALUES ( %s )", (refreshToken,))

        # update the token last refresh time
        tokenLastRefreshTime = now

        # Update Temrinal
        print('Access and Refresh Tokens refreshed');

# function to get accounts
def getRefreshAccounts( mycursor ):

    global accessToken

    # make sure we have a current access token
    checkTimeGetAccessToken( )

    print('')
    print(accessToken)
    print('')
    quit()

    # chill
    time.sleep(1)

    # Make a webservice call to get the updated response token
    url = "https://api.xero.com/api.xro/2.0/Accounts"
    payload={ }
    headers = {
        "xero-tenant-id": tenantId,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + accessToken
    }

    # read the response to dictionary
    response = requests.request("GET", url, headers=headers, data=payload)
    text = response.text
    responseObj = json.loads(text) 
    
    # loop through all accounts
    for account in responseObj['Accounts']:

        # Update MySQL
        mycursor.execute("""
        REPLACE INTO Accounts ( 
            accountId, 
            AddToWatchlist, 
            BankAccountType, 
            Class, 
            Code, 
            Description, 
            EnablePaymentsToAccount, 
            HasAttachments, 
            Name, 
            ReportingCode, 
            ShowInExpenseClaims, 
            Status, 
            SystemAccount, 
            TaxType, 
            Type,
            dataDate 
        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) """
        , (
            account.get('AccountID'), 
            account.get('AddToWatchlist'), 
            account.get('BankAccountType'), 
            account.get('Class'), 
            account.get('Code'),
            account.get('Description'),
            account.get('EnablePaymentsToAccount'),
            account.get('HasAttachments'),
            account.get('Name'),
            account.get('ReportingCode'),
            account.get('ShowInExpenseClaims'),
            account.get('Status'),
            account.get('SystemAccount'),
            account.get('TaxType'),
            account.get('Type'),
            datetime.datetime.now().strftime("%Y-%m-%d"),
        ))
        
    # Update Terminal
    print("Chart of Accounts updated")

# get tracking categories
def getTrackingCategories( mycursor ):

    global accessToken

    # make sure we have a current access token
    checkTimeGetAccessToken( )

    # Make a webservice call to get the updated response token
    url = "https://api.xero.com/api.xro/2.0/TrackingCategories"
    payload={ }
    headers = {
        "xero-tenant-id": tenantId,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + accessToken
    }

    # read the response to dictionary
    response = requests.request("GET", url, headers=headers, data=payload)
    text = response.text
    responseObj = json.loads(text) 

    pp.pprint(responseObj)
    quit()


# gets & refreshes journals
def getRefreshJournals( mycursor ):

    # scope in global access token
    global accessToken

    # get the modified since date
    modifiedSince = datetime.datetime.now() - datetime.timedelta(days = lookbackDays)

    # declare the offset variable
    offset = 0;

    # declare the continue variable in the loop
    continueLoop = True;

    # loop through API calls
    while continueLoop:

        # chill
        time.sleep(1)

        # make sure we have a current access token
        checkTimeGetAccessToken( )

        # update terminal
        print('Starting Journals offset: ' + str(offset) )

        # Make a webservice call to get the updated response token
        url = "https://api.xero.com/api.xro/2.0/Journals"
        payload={ }
        params = { 'offset': offset }
        headers = {
            "xero-tenant-id": tenantId,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + accessToken,
            "If-Modified-Since": modifiedSince.strftime("%Y-%m-%d")
        }

        # read the response to dictionary
        response = requests.request("GET", url, headers=headers, data=payload, params=params )
        text = response.text
        responseObj = json.loads(text) 

        # if we've run out of records, then get out of the loop
        if len(responseObj['Journals']) < 1:
            continueLoop = False;
            print('No more journals to collect, exiting loop')
        
        # loop through all journals
        for journal in responseObj['Journals']:

            # get the dates
            journalDate = datetime.datetime.fromtimestamp( int(journal['JournalDate'][6:-7]) / 1000 )
            createdDate = datetime.datetime.fromtimestamp( int(journal['CreatedDateUTC'][6:-7]) / 1000 )

            # update the offset
            offset = journal.get('JournalNumber')

            # Update MySQL
            mycursor.execute("""
            REPLACE INTO Journals (
                journalId, 
                journalDate, 
                journalNumber, 
                createdDateUTC, 
                reference, 
                SourceID, 
                SourceType,
                dataDate
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s ) """
            , (
                journal.get('JournalID'), 
                journalDate.strftime("%Y-%m-%d"), 
                journal.get('JournalNumber'), 
                createdDate.strftime("%Y-%m-%d"), 
                journal.get('reference'), 
                journal.get('SourceID'), 
                journal.get('SourceType'), 
                datetime.datetime.now().strftime("%Y-%m-%d"),
            ))

            # loop through all journal lines
            for journalLine in journal.get('JournalLines'):

                # Update MySQL with this journal line
                mycursor.execute("""
                REPLACE INTO Journal_Lines ( 
                    journalLineId, 
                    accountId, 
                    accountCode, 
                    accountType, 
                    accountName, 
                    description, 
                    netAmount, 
                    grossAmount, 
                    taxAmount,
                    dataDate,
                    journalId
                ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"""
                , (
                    journalLine.get('JournalLineID'), 
                    journalLine.get('AccountID'), 
                    journalLine.get('AccountCode'), 
                    journalLine.get('AccountType'), 
                    journalLine.get('AccountName'), 
                    journalLine.get('Description'), 
                    journalLine.get('NetAmount'), 
                    journalLine.get('GrossAmount'), 
                    journalLine.get('TaxAmount'), 
                    datetime.datetime.now().strftime("%Y-%m-%d"),
                    journal.get('JournalID')
                ))
                    

    # update Terminal
    print("Journals updated!")

# gets the profit & loss report for the past 12 months
def profitLossReport( mycursor, cash=False ):

    # declare date ranges
    startDate = datetime.datetime.now().replace(day=1) - relativedelta(years=1);

    # declare the table name
    tableName = 'profitLoss'
    if cash == True:
        print('This is a cash PL run')
        tableName = 'profitLoss_cash'

    # while loop for API calls
    while startDate <= datetime.datetime.now(): 

        # make sure we have a current access token
        checkTimeGetAccessToken( )

        # truncate the end date to the end of this month
        endDate = startDate + relativedelta(day=31)

        # chill
        time.sleep(1)

        # update terminal
        print('Starting the Profit & Loss Report for ' + startDate.strftime("%b %Y") )

        # Make a webservice call to get the updated response token
        url = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"
        payload={ }
        params = { 'standardLayout': True, 'paymentsOnly': cash, 'fromDate': startDate.strftime("%Y-%m-%d"), 'toDate': endDate.strftime("%Y-%m-%d") }
        headers = {
            "xero-tenant-id": tenantId,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + accessToken,
        }

        # read the response to dictionary
        response = requests.request("GET", url, headers=headers, data=payload, params=params )
        text = response.text
        responseObj = json.loads(text) 

        # delete existing data for this month
        mycursor.execute("""DELETE FROM """ + tableName + """ WHERE date = %s""", (startDate.strftime("%Y-%m-%d"),))

        # Loop through all rows in the report
        for row in responseObj['Reports'][0]['Rows']:

            # If this is a section
            if row['RowType'] == 'Section':

                # then loop through each value row
                for valueRow in row['Rows']:

                    # if this is an ordinary row
                    if valueRow['RowType'] == 'Row':

                        # jump over PL records that don't have a UUID
                        try:
                            valueRow['Cells'][1]['Attributes'][0]['Value']
                        except:
                            continue
                            
                        # then get the account UUID, value and create a unique id
                        accountUUID = valueRow['Cells'][1]['Attributes'][0]['Value']
                        value = valueRow['Cells'][1]['Value']
                        uniqueId = accountUUID + '---' + startDate.strftime("%Y-%m-%d")

                        # Update MySQL
                        mycursor.execute("""
                        INSERT INTO """ + tableName + """ ( 
                            uniqueId, 
                            accountUUID, 
                            date, 
                            value, 
                            dataDate ) 
                        VALUES ( %s, %s, %s, %s, %s )""", (
                            uniqueId, 
                            accountUUID, 
                            startDate.strftime("%Y-%m-%d"), 
                            value,
                            datetime.datetime.now().strftime("%Y-%m-%d"),
                        ))


        # increment the start date
        startDate = startDate + relativedelta(months=1)
        
# gets the profit & loss report for the past 12 months
def balanceSheetReport( mycursor ):

    # declare date ranges
    startDate = datetime.datetime.now().replace(day=1) - relativedelta(years=1);

    # while loop for API calls
    while startDate <= datetime.datetime.now(): 

        # make sure we have a current access token
        checkTimeGetAccessToken( )

        # truncate the end date to the end of this month
        endDate = startDate + relativedelta(day=31)

        # chill
        time.sleep(1)

        # update terminal
        print('Starting the Balance Sheet Report for ' + startDate.strftime("%b %Y") )

        # Make a webservice call to get the updated response token
        url = "https://api.xero.com/api.xro/2.0/Reports/BalanceSheet"
        payload={ }
        params = { 'standardLayout': True, 'date': endDate.strftime("%Y-%m-%d") }
        headers = {
            "xero-tenant-id": tenantId,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + accessToken,
        }

        # read the response to dictionary
        response = requests.request("GET", url, headers=headers, data=payload, params=params )
        text = response.text
        responseObj = json.loads(text) 

        # delete existing data for this month
        mycursor.execute("""DELETE FROM balanceSheet WHERE date = %s""", (endDate.strftime("%Y-%m-%d"),))

        # Loop through all rows in the report
        for row in responseObj['Reports'][0]['Rows']:

            # If this is a section
            if row['RowType'] == 'Section':

                # then loop through each value row
                for valueRow in row['Rows']:

                    # if this is an ordinary row
                    if valueRow['RowType'] == 'Row':

                        # jump over PL records that don't have a UUID
                        try:
                            valueRow['Cells'][1]['Attributes'][0]['Value']
                        except:
                            continue
                            
                        # then get the account UUID, value and create a unique id
                        accountUUID = valueRow['Cells'][1]['Attributes'][0]['Value']
                        value = valueRow['Cells'][1]['Value']
                        uniqueId = accountUUID + '---' + endDate.strftime("%Y-%m-%d")

                        # Update MySQL
                        mycursor.execute("""
                        INSERT INTO balanceSheet ( 
                            uniqueId, 
                            accountUUID, 
                            date, 
                            value, 
                            dataDate ) 
                        VALUES ( %s, %s, %s, %s, %s )""", (
                            uniqueId, 
                            accountUUID, 
                            endDate.strftime("%Y-%m-%d"), 
                            value,
                            datetime.datetime.now().strftime("%Y-%m-%d"),
                        ))


        # increment the start date
        startDate = startDate + relativedelta(months=1)
        
        
#############
## Runtime ##
#############

# get Mysql connection
mydb = mysqlConnect( )
mycursor = mydb.cursor(dictionary=True)

# start osservare job monitor
today = datetime.datetime.now() 
mydict = { "nome": "orbe_xero", "inizio": today, "inAvanti": True }
_id = durataCollection.insert_one( mydict )

# get tracking categories
# getTrackingCategories( mycursor )

# get the Chart of Accounts
getRefreshAccounts( mycursor )

# get Journals
getRefreshJournals( mycursor )

# get Profit & Loss Report
profitLossReport( mycursor )

# get Profit & Loss Cash Report
profitLossReport( mycursor, True )

# get Balance Sheet Report
balanceSheetReport( mycursor )

# Job done
print('All Done!')

# end osservare job monitor
today = datetime.datetime.now() 
durataCollection.update_one({'_id':ObjectId(_id.inserted_id)}, {"$set": {"fine": today, "inAvanti": False}})

