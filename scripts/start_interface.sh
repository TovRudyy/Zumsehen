export FLASK_ENV=development
SECRET_KEY=$(uuidgen)
export SECRET_KEY=SECRET_KEY
export FLASK_APP=src/interface/interface.py
flask run