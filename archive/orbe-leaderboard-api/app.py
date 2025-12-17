#
# Gets called automatically by Flask to setup the application
#

from cgi import test
from flask import Flask, session
from flask_cors import CORS
from blueprint_auth import blueprint_auth
from blueprint_settings import blueprint_settings
from blueprint_leaderboard import blueprint_leaderboard
from functions import *
from variables import *


# define the flask app and secret.  Set the CORS policy
app = Flask(__name__)
app.secret_key = "Puys8vM%k9#O71#XHxupKs!sBu6wl#TW5a8j%#Zja6JT@z1^SY"
CORS(app)

# register the auth blueprint
app.register_blueprint(blueprint_auth)

# register the settings blueprint
app.register_blueprint(blueprint_settings)

# register the leaderboard blueprint
app.register_blueprint(blueprint_leaderboard)


# debug endpoint
@app.route('/read', methods=['GET'])
@auth.login_required
def read():
    username = session['token']
    return username

