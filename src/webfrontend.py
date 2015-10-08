import os
import sys

sys.path.append(os.path.dirname(__name__))

from app import app

app.run(debug=True)
