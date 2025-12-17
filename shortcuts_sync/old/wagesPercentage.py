#
# This script is designed to calcualte total wages as a percentage of total income on a weekly basis
# We grab data for the current week and attribute it to the previous week - because Sales and Payroll are 1 week beind (I do the books on a Tuesday for the week just gone)
# We use Xero as the source of truth to calculate this
#
#

import mysql.connector
import pprint
import datetime
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import timedelta

# create mysql database connection
mydb = mysql.connector.connect(
  host="206.189.150.30",
  user="ONA",
  password="aqCGp?wW2c*Xz9V-",
  autocommit=True
)

# connect to mongoDb in Digital Ocean
digitalOceanMongo = MongoClient('206.189.150.30',username='talis.evans',password='tes02029~~~')

# declare the mongodb database and collection to start the job
dbmonitor = digitalOceanMongo.osservare
durataCollection = dbmonitor['durata']
erroreCollection = dbmonitor['errore']
today = datetime.datetime.now() 


################
## Functions ##
##############

# declare a date object for 3 months in the past
def threeMonthsAgo():
    today = datetime.datetime.now()
    threeMonthsAgo = today - datetime.timedelta(days=90)
    return threeMonthsAgo

# declare a function to get Xero data from the MySQL database
def getXeroData( startDate, accountUUIDs, isCredit=False ):

    # declare the output dictionary
    output = {}

    # declare the cursor
    mycursor = mydb.cursor()

    # Prepare the SQL query with placeholders for accountUUIDs
    query = """
        SELECT
            DATE(journalDate) AS journalDate,
            SUM(netAmount) AS amount
        FROM ONA_Xero.Journals
        INNER JOIN ONA_Xero.Journal_Lines ON Journals.journalId = Journal_Lines.journalId
        WHERE DATE(journalDate) > DATE(%s)
        AND Journal_Lines.accountId IN ({})
        GROUP BY DATE(journalDate)
    """.format(','.join(['%s'] * len(accountUUIDs)))

    # Build a tuple of parameters for the placeholders
    params = (startDate,) + tuple(accountUUIDs)

    # Execute the query
    mycursor.execute(query, params)

    # Fetch the results
    results = mycursor.fetchall()

    # loop through the results
    for row in results:
        
        # get the journal date
        journalDate = row[0]

        # get the amount
        amount = row[1]
        if isCredit:
            amount = -amount

        # modify the journalDate to the previous isoWeek
        previous_iso_week = journalDate - timedelta(days=7)

        # get the iso year and iso week of previous_iso_week
        isoYear = previous_iso_week.isocalendar()[0]
        isoWeek = previous_iso_week.isocalendar()[1]

        # add the amount to the output dictionary
        if isoYear not in output:
            output[isoYear] = {}

        # declare the iso week
        if isoWeek not in output[isoYear]:
            output[isoYear][isoWeek] = 0

        # add to the output
        output[isoYear][isoWeek] += amount

    # return the output
    return output

# build the output dataset
def buildOutputDataset( startDate, xeroSalesData, xeroWagesData ):

    # declare the compiled dataset
    compiledDataset = []

    # modify the start date to 7 days in the past
    startDate = startDate - timedelta(days=7)

    # declare a variable for today less 7 days
    today = datetime.datetime.now()

    # loop through each iso week between startDate and now
    while startDate < today:

        # get the current iso year and iso week
        isoYear = startDate.isocalendar()[0]
        isoWeek = startDate.isocalendar()[1]

        # make a date object from the isoYear and isoWeek variables
        dateObject = datetime.datetime.strptime(str(isoYear) + "-" + str(isoWeek) + "-1", "%G-%V-%u")

        # update the dateObject to be the last day of the week, with a week ending on Sunday
        dateObject = dateObject + timedelta(days=6)

        # get the total sales for the week if it exists
        totalSales = 0
        if isoYear in xeroSalesData and isoWeek in xeroSalesData[isoYear]:
            totalSales = xeroSalesData[isoYear][isoWeek]

        # get the total wages for the week if it exists
        totalWages = 0
        if isoYear in xeroWagesData and isoWeek in xeroWagesData[isoYear]:
            totalWages = xeroWagesData[isoYear][isoWeek]

        # append to the compiled dataset
        compiledDataset.append({
            "weekEndingDate": dateObject,
            "totalSales": totalSales,
            "totalWages": totalWages,
            "dataDate": datetime.datetime.now()
        });

        # increment the startDate by 7 days
        startDate = startDate + timedelta(days=7)

    # return the compiled dataset
    return compiledDataset

# upload the output data
def uploadOutputData( outputDataset ):
    
    # declare the cursor
    mycursor = mydb.cursor()

    # Prepare the SQL query with placeholders for accountUUIDs
    query = """
        REPLACE INTO ONA_Xero.Wages_Percentage (
            weekEndingDate,
            totalSales,
            totalWages,
            dataDate
        ) VALUES (
            %s,
            %s,
            %s,
            %s
        )
    """

    # Build a tuple of parameters for the placeholders
    params = []
    for row in outputDataset:
        params.append((
            row['weekEndingDate'],
            round(row['totalSales'],2),
            round(row['totalWages'],2),
            row['dataDate']
        ))

    # Execute the query
    mycursor.executemany(query, params)

    # Commit the changes
    mydb.commit()

##############
## Runtime ##
############

# declare the accounts that are 'sales' accounts in Xero
xeroSalesAccountUUIDs = [
    '8c5f8d7f-140d-433d-9502-9caf70a330d5', # Salon Takings
    '7d2ee2b0-7c2c-49d1-a912-8a54dea05436' # Sales Reconciliation Adjustments
]

# declare the accounts that are 'wages' accounts in Xero
xeroWagesAccountUUIDs = [
    '8bb8e313-34f4-42b6-812c-d15abdae844c',# Wages and Salaries
    '602de599-7459-47bd-8df2-5affef7cda2b' # Superannuation
]

# start osservare job monitor
mydict = { "nome": "orbeWagesPercentage", "inizio": today, "inAvanti": True }
_id = durataCollection.insert_one( mydict )

# refresh the last 3 months' data in every run
startDate = threeMonthsAgo( )

# update Terminal with reference to the startDate
print("Getting sales data from " + str(startDate))

# get sales data (returns a dictionary by isoYear and isoWeek) - calculated to the previous ISO Week
xeroSalesData = getXeroData( startDate, xeroSalesAccountUUIDs, True )

# update Terminal with reference to the startDate
print("Getting wages data from " + str(startDate))

# get Xero wages data
xeroWagesData = getXeroData( startDate, xeroWagesAccountUUIDs, False )

# update Terminal
print("Building output dataset")

# build the output dataset
outputDataset = buildOutputDataset( startDate, xeroSalesData, xeroWagesData )

# update Terminal
print("Inserting output dataset into MySQL")

#upload the output dataset
uploadOutputData( outputDataset )

# end osservare job monitor
today = datetime.datetime.now() 
durataCollection.update_one({'_id':ObjectId(_id.inserted_id)}, {"$set": {"fine": today, "inAvanti": False}})

# update Terminal
print("All done!")

