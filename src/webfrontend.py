import os
import sys

sys.path.append(os.path.dirname(__name__))

from app import app
from settings import *

app.run(host=default_webfrontend['host'], port=default_webfrontend['port'], debug=True)
