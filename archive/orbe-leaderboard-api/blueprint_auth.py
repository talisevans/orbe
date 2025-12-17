

#
# This holds the functionality to login users and generate tokens
#


from flask import Blueprint, session, request
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from flask_session import Session
import random
from functions import *
from variables import *

# declare the auth blueprint
blueprint_auth = Blueprint('blueprint_auth', __name__)

# function to login
@blueprint_auth.route('/login')
@login.login_required
def loginMethod():

    # get a mysql connection
    mysql = get_mysql()
    mysql_cursor = mysql.cursor()

    # declare the expiry time and the time to clear old tokens.  Clear tokens older than 10 days
    tokenExpiry = datetime.now()+timedelta(days=7)
    clearOldTokens = datetime.now()-timedelta(days=10)

    # get these values as epoch timestamps
    epoch_tokenExpiry = int(tokenExpiry.timestamp())
    epoch_clearOldTokens = int(clearOldTokens.timestamp())

    # get the current logged in user
    user = login.current_user()

    # get the user's IP address
    ipAddress = request.remote_addr

    # generate the token
    generatedToken = generate_token( 100 )

    # delete old tokens
    mysql_cursor.execute("DELETE FROM leaderboardAPI WHERE expiry < (%s)", [ epoch_clearOldTokens ])
    
    # insert my new token
    mysql_cursor.execute("INSERT INTO leaderboardAPI (token, expiry, ipAddress, user) VALUES ( %s, %s, %s, %s )", [ generatedToken, epoch_tokenExpiry, ipAddress, user ])

    return {
        "token": generatedToken,
        "expiry": epoch_tokenExpiry,
    }
