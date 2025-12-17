import sys
import logging 

logging.basicConfig (level=logging. DEBUG, filename='/var/www/html/orbe-leaderboard-api/logs/orbe-leaderboard.log', format='%(asctime)s %(message)s')
sys.path. insert (0,'/var/www/html/orbe-leaderboard-api')
sys.path. insert (0,'/var/www/html/orbe-leaderboard-api/lib/python3.10/site-packages') 
import app as application