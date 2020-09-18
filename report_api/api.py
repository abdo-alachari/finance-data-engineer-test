from flask import Flask, request, jsonify
import json

import utils

app = Flask(__name__)



@app.route("/monthly-report", methods=["GET"])
def monthly_report():
    content = request.args
    user_input = {'user_id':content["user_id"]}

    user_id = user_input['user_id']      

    json_data = utils.monthly_report(user_id)      

    return jsonify(json_data) 


@app.route("/daily-report", methods=["GET"])
def daily_report():
    content = request.args
    user_input = {'user_id':content["user_id"]}

    user_id = user_input['user_id']      

    json_data = utils.daily_report(user_id)      

    return jsonify(json_data) 


@app.route("/total-report", methods=["GET"])
def total_report():
    content = request.args
    user_input = {'user_id':content["user_id"]}

    user_id = user_input['user_id']      

    json_data = utils.total_report(user_id)      

    return jsonify(json_data) 


if __name__ == '__main__':

    app.run(port=4043,host="0.0.0.0")