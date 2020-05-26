import logging
import os
import uuid

from flask import Flask

from src.interface import routes

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", uuid.uuid4())
