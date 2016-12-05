import os
import sys

sys.path.append(os.path.dirname(__name__))

from app import app
from settings import DEFAULT_WEB_SERVER

app.run(host=DEFAULT_WEB_SERVER['host'], port=DEFAULT_WEB_SERVER['port'], debug=True)
