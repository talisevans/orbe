#
# This holds the functionality for endpoints associated with the leaderboard
#

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import json
from functions import *
from variables import *
import pprint

# declare the settings blueprint
blueprint_leaderboard = Blueprint('blueprint_leaderboard', __name__)

# get staff leaderboard data
@blueprint_leaderboard.route('/leaderboard', methods=['GET'])
@auth.login_required
def getLeaderboard():

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor( dictionary=True )

    # get a list of staff we should be including in the leaderboard with their targets
    includedStaff = dbQueryGetStaffSettings( mysql_cursor );

    # declare an ETL variable, using employeeId as key
    leaderboardPrepETL = {}

    # create an employee names lookup dictionary
    employeeNamesDict = {}

    # loop through all staff in our targets list
    for selectedStaff in includedStaff:

        # jump over any staff that aren't meant to be included
        if selectedStaff['showEmployee'] == False:
            continue

        # add this employee to the employee names dict
        employeeNamesDict[ str(selectedStaff['EmployeeId']) ] = selectedStaff['employeeName']
            
        # if we don't yet have this employeeId in the dictionary, then create them,
        if selectedStaff['EmployeeId'] not in leaderboardPrepETL:
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])] = {}
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['revenue'] = {}
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['rebooking'] = {}
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['products'] = {}
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['products']['units'] = 0
            leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['products']['revenue'] = 0


        # add the targets for this employee
        leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['revenue']['target'] = selectedStaff['targetIncome']
        leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['revenue']['actual'] = 0  # to be worked out in the dbGetStaffActuals function below
        leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['rebooking']['target'] =  float(round(selectedStaff['targetRebookings']/100, 2))
        leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['rebooking']['actual'] = 0  # to be worked out in the dbGetStaffActuals function below

        # there is a standard amount for 'products' at the moment.  In the future, this might need to be hard coded
        leaderboardPrepETL[str(selectedStaff['EmployeeId'])]['products']['unitsTarget'] = 10

    
    # get the current week start on a Monday
    weekStart = datetime.today()  - timedelta(days=datetime.today().weekday() % 7)
    now = datetime.today();
    
    # add 1 day to make the week start Tuesday (unique to Orbe Nth Adl)
    weekStart = weekStart + timedelta(days=1);
    weekStart = weekStart.replace(hour=9, minute=0) # start the work week at 9am
    weekEnd = weekStart + timedelta(days=4);
    weekEnd = weekEnd.replace(hour=17, minute=0) # end the work week at 5pm

    # calculate the difference 
    diff = now - weekStart
    days, seconds = diff.days, diff.seconds
    hoursDiff = days * 24 + seconds // 3600

    # The normal Orbe work week (between 9am on Tue to 5pm on Sat) is 104 hours
    # if the working week is more than 100% completed, then stick to 100%, otherwise calculate the percentage of the week completed
    percentWeekCompleted = float(round(hoursDiff / 104,2))
    if percentWeekCompleted > 1:
        percentWeekCompleted = 1
        

    # gets staff leaderboard data from the database
    leaderboardPrepETL = dbGetStaffActuals( mysql_cursor, leaderboardPrepETL, weekStart, weekEnd )

    return {
        "response": leaderboardPrepETL,
        "type": "success",
        "percentComplete": percentWeekCompleted,
        "employeeNamesDictionary": employeeNamesDict
    }


# get staff leaderboard data
@blueprint_leaderboard.route('/rebooking-sync', methods=['GET'])
@auth.login_required
def rebookingSync():

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor( dictionary=True )

    # resync the rebooking table
    resyncRebookingTable( mysql_cursor )

    return {
        "response": "success"
    } 

