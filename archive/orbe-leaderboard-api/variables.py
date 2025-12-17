
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import random
import pymysql
pymysql.install_as_MySQLdb()

# define Orbe North Adelaide user
currentYear = datetime.now().strftime('%Y')
currentMonth = datetime.now().strftime('%b')


# declare login - basic to generate a token, then use the auth object to ineract with the token
login = HTTPBasicAuth()
auth = HTTPTokenAuth(scheme='Bearer')

# declare the mysql engine.  This is used to send the dataframe to sql
from sqlalchemy import create_engine
engine = create_engine("mysql+mysqldb://ONA:"+'aqCGp?wW2c*Xz9V-'+"@206.189.150.30/Shortcuts")


# declare available users
users = {
    "orbeNorthAdelaide": generate_password_hash("Tara1992" + currentMonth + currentYear ),
    # "test": generate_password_hash("test" ),
}

