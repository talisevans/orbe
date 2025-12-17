#
#  This script is run on a daily basis and calculates the bonus structure for Orbe employees
#
#

# library imports
import mysql.connector
import pprint
import datetime
import isoweek
from pymongo import MongoClient
from bson.objectid import ObjectId

# declare bonus structure values  ** Change these here **
bonusStructure = {
    "revenueTargets" : [
        {
            "target" : 2500,
            "bonus" : 75
        },
        {
            "target" : 3000,
            "bonus" : 100
        },
        {
            "target" : 3500,
            "bonus" : 125
        },
        {
            "target" : 4000,
            "bonus" : 150
        },
        {
            "target" : 4500,
            "bonus" : 175
        },
        {
            "target" : 5000,
            "bonus" : 200
        },
        {
            "target" : 5500,
            "bonus" : 225
        },
        {
            "target" : 6000,
            "bonus" : 250
        },
        {
            "target" : 6500,
            "bonus" : 275
        },
        {
            "target" : 7000,
            "bonus" : 300
        },
        {
            "target" : 7500,
            "bonus" : 325
        },
        {
            "target" : 8000,
            "bonus" : 350
        }
    ],
    "productTargets" : {
        "target": 300,  # if you sell at least $300 retail products
        "bonus": 0.1  # you get a 10% of the value of what you've sold
    },
    "rebookingTarget" : {
        "target": 0.75,  # if you hit 75% rebooking
        "bonus": 50 # you get a $50 bonus
    }
}

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

# function to get transaction data from mysql
def getData( mydb ):

    # mysql query to get transaction data
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("""
        SELECT
            ItemTypeStringCode,
            EmployeeId,
            DATE(TransactionDate) AS transactionDate,
            SUM(LineIncTaxAmount) AS LineIncTaxAmount,
            SUM(ItemQuantity) AS itemQuantity
        FROM Shortcuts.SaleTransactionLine
        WHERE ItemTypeStringCode IN ( 'ItemType.Service', 'ItemType.Product' )
        AND DATE(TransactionDate) >= DATE_SUB( DATE(NOW()), INTERVAL 6 MONTH )  # look back 6 months
        GROUP BY ItemTypeStringCode, EmployeeId, DATE(TransactionDate)
        ORDER BY DATE(TransactionDate) DESC;
    """)

    # get all transactions
    transactions = mycursor.fetchall()

    # declare a variable for ETL
    etl =  {}

    # loop through transactions
    for transaction in transactions:

        # get the ISO year from the transactionDate key
        transactionYear = transaction["transactionDate"].isocalendar()[0]
        transactionWeek = transaction["transactionDate"].isocalendar()[1]

        # build date object from OS year and ISO week starting at ISO weekday 1
        weekStart = isoweek.Week(transactionYear, transactionWeek).monday();
        weekEnd = isoweek.Week(transactionYear, transactionWeek).sunday();

        # get weekStart and weekEnd as strings
        weekStart = weekStart.strftime("%d %b %Y")
        weekEnd = weekEnd.strftime("%d %b %Y")

        # declare period string
        period = weekStart + " - " + weekEnd

        # if we don't yet have this weekStart declared in the etl variable, the add it
        if weekStart not in etl:
            etl[weekStart] = {}

        # if we don't yet have this employeeId declared in the etl variable, the add it
        if str(transaction["EmployeeId"]) not in etl[weekStart]:
            etl[weekStart][str(transaction["EmployeeId"])] = {}

        # if we don't yet have this ItemTypeStringCode declared in the etl variable, the add it
        if transaction["ItemTypeStringCode"] not in etl[weekStart][str(transaction["EmployeeId"])]:
            etl[weekStart][str(transaction["EmployeeId"])][transaction["ItemTypeStringCode"]] = {
                "LineIncTaxAmount": 0,
                "itemQuantity": 0 
            }

        # add the LineIncTaxAmount and itemQuantity to the etl variable
        etl[weekStart][str(transaction["EmployeeId"])][transaction["ItemTypeStringCode"]]["LineIncTaxAmount"] += transaction["LineIncTaxAmount"]
        etl[weekStart][str(transaction["EmployeeId"])][transaction["ItemTypeStringCode"]]["itemQuantity"] += transaction["itemQuantity"]

    return {
        "etl": etl
    }

# function to get rebookings
def getRebookings( mydb ):

    # mysql query to get transaction data
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("""

        SELECT
            COUNT( DISTINCT CONCAT(clientId,'---',visitDate) ) AS numVisits,
            SUM(isRebooked) AS numRebookings,
            visitDate,
            employeeId
        FROM Shortcuts.rebookingOutput
        WHERE DATE(visitDate) >= DATE(DATE_SUB(NOW(), INTERVAL 6 MONTH))
        GROUP BY employeeId, visitDate
    """)

    # get all transactions
    rebookings = mycursor.fetchall()

    # declare a variable for ETL
    etl =  {}

    # loop through transactions
    for rebooking in rebookings:

        # get the ISO year from the visitDate key
        rebookingYear = rebooking["visitDate"].isocalendar()[0]
        rebookingWeek = rebooking["visitDate"].isocalendar()[1]

        # build date object from OS year and ISO week starting at ISO weekday 1
        weekStart = isoweek.Week(rebookingYear, rebookingWeek).monday();
        weekEnd = isoweek.Week(rebookingYear, rebookingWeek).sunday();

        # get weekStart and weekEnd as strings
        weekStart = weekStart.strftime("%d %b %Y")
        weekEnd = weekEnd.strftime("%d %b %Y")

        # declare period string
        period = weekStart + " - " + weekEnd

        # if we don't yet have this weekStart declared in the etl variable, the add it
        if weekStart not in etl:
            etl[weekStart] = {}

        # if we don't yet have this employeeId declared in the etl variable, the add it
        if str(rebooking["employeeId"]) not in etl[weekStart]:
            etl[weekStart][str(rebooking["employeeId"])] = {
                "numVisits": 0,
                "numRebookings": 0
            }

        etl[weekStart][str(rebooking["employeeId"])]["numVisits"] += rebooking["numVisits"]
        etl[weekStart][str(rebooking["employeeId"])]["numRebookings"] += rebooking["numRebookings"]
        
    return {
        "etl": etl
    }

# function to calculate bonuses
def calcBonuses( data, bonusStructure, employees ):

    # declare the output dataset
    outputDataset = [ ];

    # create a date object 6 months ago
    sixMonthsAgo = datetime.datetime.now() - datetime.timedelta(days=180)

    # loop through each week in the last 6 months
    while sixMonthsAgo <= datetime.datetime.now():

        # get the ISO year from the transactionDate key
        transactionYear = sixMonthsAgo.isocalendar()[0]
        transactionWeek = sixMonthsAgo.isocalendar()[1]
        weekStart = isoweek.Week(transactionYear, transactionWeek).monday();
        weekEnd = isoweek.Week(transactionYear, transactionWeek).sunday();
    
        # get the week start, week end and period strings
        weekStart = weekStart.strftime("%d %b %Y")
        weekEnd = weekEnd.strftime("%d %b %Y")
        period = weekStart + " - " + weekEnd

        # if we have data for this week
        if weekStart in data["etl"]:

            # loop through each employee
            for employeeId in employees:

                # if we have data for this employee
                if employeeId in data["etl"][weekStart]:


                    ####################
                    ## Revenue Bonus ##
                    ###################

                    # by default, there is no revenue bonus
                    revenueBonus = 0

                    # get total services income for this employee
                    totalServicesIncome = 0

                    if "ItemType.Service" in data["etl"][weekStart][employeeId]:
                        totalServicesIncome = data["etl"][weekStart][employeeId]["ItemType.Service"]["LineIncTaxAmount"]

                    # loop through the revenue targets
                    for revenueTarget in bonusStructure["revenueTargets"]:
                        if totalServicesIncome >= revenueTarget["target"] and revenueTarget["bonus"] > revenueBonus:
                            revenueBonus = revenueTarget["bonus"]


                    ####################
                    ## Product Bonus ##
                    ###################

                    # by default, there is no product bonus
                    productBonus = 0

                    # get total products quantity for this employee
                    totalProductsQuantity = 0
                    totalProductsIncome = 0
                    if "ItemType.Product" in data["etl"][weekStart][employeeId]:
                        totalProductsQuantity = data["etl"][weekStart][employeeId]["ItemType.Product"]["itemQuantity"]
                        totalProductsIncome = data["etl"][weekStart][employeeId]["ItemType.Product"]["LineIncTaxAmount"]

                    # if the employee has sold at least 10 products, they get a 10% bonus
                    if totalProductsIncome >= bonusStructure["productTargets"]["target"]:
                        productBonus = bonusStructure["productTargets"]["bonus"] * totalProductsIncome


                    #####################
                    ## Rebooking Bonus ##
                    #####################

                    # declare the rebooking percentage
                    rebookingPercentage = 0
                    rebookingBonus = 0

                    # if we have rebooking data for this employee
                    if "rebookings" in data["etl"][weekStart][employeeId]:
                        totalNumRebookings = data["etl"][weekStart][employeeId]["rebookings"]["numRebookings"]
                        totalNumVisits = data["etl"][weekStart][employeeId]["rebookings"]["numVisits"]

                        # calculate the rebooking percentage
                        if totalNumVisits > 0 and totalNumRebookings > 0:
                            rebookingPercentage = totalNumRebookings / totalNumVisits

                        # if the rebooking percentage is greater than the target, set the rebooking bonus to the bonus amount
                        if rebookingPercentage >= bonusStructure["rebookingTarget"]["target"] and totalServicesIncome > 1500:
                            rebookingBonus = bonusStructure["rebookingTarget"]["bonus"]

                    
                    ####################
                    ## Employee Name ##
                    ##################
                    
                    # check to see if this employee exists in the employees dictionary
                    employeeName = ""
                    if employeeId in employees:
                        employeeName = employees[employeeId]

                    # append values to the output dataset
                    outputDataset.append({
                        "weekStartDate": weekStart,
                        "period": period,
                        "employeeId": employeeId,
                        "employeeName": employeeName,
                        "totalServicesIncome": round(totalServicesIncome,2),
                        "totalProductsQuantity": round(totalProductsQuantity,0),
                        "totalProductsIncome": round(totalProductsIncome,2),
                        "rebookingPercentage": round(rebookingPercentage,2),
                        "revenueBonus": round(revenueBonus,2),
                        "productBonus": round(productBonus,2),
                        "rebookingBonus": round(rebookingBonus,2),
                        "totalBonus": round(revenueBonus + productBonus + rebookingBonus,2)
                    })

        # bring sixMonthsAgo forward to the next iso week
        sixMonthsAgo = sixMonthsAgo + datetime.timedelta(days=7)
        
    return outputDataset

# get a dictionary of employees
def getEmployeeLookup( mydb ):

    # declare employees dictionary
    employeesDict = {}

    # mysql query to get transaction data
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("""
        SELECT employeeId, CONCAT(FirstName,' ',Surname) AS employeeName FROM Shortcuts.EmployeeSite;
    """)

    # get all transactions
    employees = mycursor.fetchall()

    # loop through employees
    for employee in employees:

        # add employee to dictionary
        employeesDict[str(employee["employeeId"])] = employee["employeeName"]

    return employeesDict

# update mysql with bonus data
def updateMySQL( dataWithBonusCalcs, mydb ):

    # declare variable for mysql formatted dataset
    mysqlDataset = []

    # loop through each row in the dataWithBonusCalcs variable
    for row in dataWithBonusCalcs:

        # append totalServicesIncome to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "1. Services Income",
            "value": row["totalServicesIncome"]
        })

        # append totalProductsIncome to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "2. Products Income",
            "value": row["totalProductsIncome"]
        })

        # append totalProductsQuantity to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "3. Products Quantity",
            "value": row["totalProductsQuantity"]
        })

        # append rebookingPercentage to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "4. Rebooking Percent",
            "value": row["rebookingPercentage"]
        })

        # append revenue bonuses to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "5. Services Bonus",
            "value": row["revenueBonus"]
        })

        # append product bonuses to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "6. Products Bonus",
            "value": row["productBonus"]
        })

        # append rebooking bonuses to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "7. Rebooking Bonus",
            "value": row["rebookingBonus"]
        })

        # append total bonuses to mysqlDataset
        mysqlDataset.append({
            "weekStartDate": row["weekStartDate"],
            "period": row["period"],
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "measure": "8. Total Bonus",
            "value": row["totalBonus"]
        })

    # delete existing data in the employeeBonuses table
    mycursor = mydb.cursor()
    mycursor.execute("DELETE FROM Shortcuts.employeeBonuses;")

    # get the current datetime as a string
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")

    # loop through each row in the dataWithBonusCalcs variable
    for row in mysqlDataset:

        # insert row into mysql
        mycursor = mydb.cursor()
        mycursor.execute("""
            INSERT INTO Shortcuts.employeeBonuses (
                weekStartDate,
                period,
                employeeId,
                employeeName,
                dataDate,
                measure,
                value
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            );
        """, [ row['weekStartDate'], row['period'], row['employeeId'], row['employeeName'], now, row['measure'], row['value'], ])

# join datasets
def joinData( data, rebookingData ):

    # loop through rebookingsData
    for periodStart in rebookingData['etl']:

        # loop through employees
        for employeeId in rebookingData['etl'][periodStart]:

            # check to see if this periodStart exists in the data variable
            if periodStart in data['etl']:
    
                # check to see if this employeeId exists in the data variable
                if employeeId in data['etl'][periodStart]:

                    # add rebookingData to data
                    data['etl'][periodStart][employeeId]['rebookings'] = {
                        "numVisits": rebookingData['etl'][periodStart][employeeId]['numVisits'],
                        "numRebookings": rebookingData['etl'][periodStart][employeeId]['numRebookings']
                    }

                # otherwise, if this employee doesn't exist in the data variable
                else:

                    # add employee to data
                    data['etl'][periodStart][employeeId] = {
                        "rebookings": {
                            "numVisits": rebookingData['etl'][periodStart][employeeId]['numVisits'],
                            "numRebookings": rebookingData['etl'][periodStart][employeeId]['numRebookings']
                        }
                    }

            # otherwise, if this periodStart doesn't exist in the data variable
            else:
                    
                # add periodStart to data
                data['etl'][periodStart] = {
                    employeeId: {
                        "rebookings": {
                            "numVisits": rebookingData['etl'][periodStart][employeeId]['numVisits'],
                            "numRebookings": rebookingData['etl'][periodStart][employeeId]['numRebookings']
                        }
                    }
                }

    return data

##############
## Runtime ##
############

# start osservare job monitor
mydict = { "nome": "orbeBonuses", "inizio": today, "inAvanti": True }
_id = durataCollection.insert_one( mydict )

# update Terminal
print("Getting transaction data")

# get data from mysql
data = getData( mydb )

# update Terminal
print("Getting rebookings data")

# get rebookings data
rebookingData = getRebookings( mydb )

# update Terminal
print("join transaction data with rebookings data")

# join transaction data with rebookings data
data = joinData( data, rebookingData )

# update Terminal
print("Getting employee names")

# get employees from mysql
employees = getEmployeeLookup( mydb )

# update Terminal
print("Calculating bonuses")

# calculate bonuses
dataWithBonusCalcs = calcBonuses( data, bonusStructure, employees )

# update Terminal
print("Uploading to the database")

# update mysql with bonus data
updateMySQL( dataWithBonusCalcs, mydb )

# end osservare job monitor
today = datetime.datetime.now() 
durataCollection.update_one({'_id':ObjectId(_id.inserted_id)}, {"$set": {"fine": today, "inAvanti": False}})

# update Terminal
print("All done!")


