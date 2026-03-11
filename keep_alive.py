from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "Babblebox is awake and partying! 🎈"

def run():
    # Binds to port 8080, which the cloud provider expects
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()