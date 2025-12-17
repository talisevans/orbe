#
# This holds the functionality get and update staff settings
#

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import random
import json
from functions import *
from variables import *

# declare the settings blueprint
blueprint_settings = Blueprint('blueprint_settings', __name__)

# get staff settings
@blueprint_settings.route('/get-settings', methods=['GET'])
@auth.login_required
def getStaffSettings():

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor( dictionary=True )

    # get the staff list
    staffList = dbQueryGetStaffSettings(mysql_cursor)

    # declare the response
    response = {}
    response['success'] = True;
    response['data'] = staffList

    # return the staff list
    return jsonify(response)

# update staff settings
@blueprint_settings.route('/update-settings', methods=['POST'])
@auth.login_required
def updateStaffSettings():

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor( dictionary=True )

    # get a dictionary for staff names lookup
    staffNamesLookupDict = dbStaffNamesLookup( mysql_cursor )

    # get staff data coming back from Angular to process
    staffData = request.json

    # loop through each staff member coming back
    for staffId in staffData:

        # get the staff name
        staffName = staffNamesLookupDict.get('staff-'+str(staffId))

        # get the variables to insert
        targetIncome = staffData[staffId]['targetIncome']
        targetRebookings = staffData[staffId]['targetRebookings']
        showEmployee = 'true' if staffData[staffId]['showEmployee'] == True else 'false'

        # update the value for this employee in MySQL
        mysql_cursor.execute("REPLACE INTO EmployeeTargets ( EmployeeId, employeeName, targetIncome, targetRebookings, showEmployee ) VALUES ( %s, %s, %s, %s, %s )", [ staffId, staffName, targetIncome, targetRebookings, showEmployee ] )


    return {
        "response": "success"
    }

