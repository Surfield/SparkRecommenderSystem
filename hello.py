from flask import Flask, render_template
import os
import reco
import json

app = Flask(__name__)

@app.route("/")
def hello():
    return "Explain API!"

@app.route("/product/<id>")
def product(id):
    # return "Hello World! "+ str(id)
    return render_template("products.html", recos=reco.recommend(id))

@app.route("/product_json/<id>")
def product_json(id):
    # return "Hello World! "+ str(id)
    return json.dumps(reco.recommend(id))

if __name__ == "__main__":
    app.run()