from flask import Flask, request, jsonify
import time
import json
from bson import json_util
from pymongo import MongoClient

conn_string = "<Connection_string>"
client = MongoClient(conn_string)
db = client['gds_db']
collection = db['logistics_data']

app = Flask(__name__)

@app.route('/getVehicleNo', methods=['GET'])
def getVehicleNos():
    data = request.args
    gpsProvider = data.get('GpsProvider')
    delay_status = data.get('delay')

    query = {"$and": [{"GpsProvider": gpsProvider}, {"delay": {"$regex": delay_status}}] }
    if gpsProvider:
        vehicles_list = []
        documents = collection.find(query)
        for document in documents:
            vehicles_list.append(document['vehicle_no'])
        client.close()
        return jsonify({"Vehicle Nos":vehicles_list}), 200
    else:
        return jsonify({"error": "No vehicles listed under the Gps Provider!"}), 400

@app.route('/getLogisticsDetails', methods=['GET'])
def getLogisticsData():
    data = request.args
    vehicleNo = data.get('vehicleNo')

    query = {"vehicle_no": vehicleNo}

    if vehicleNo:
        document = collection.find(query)
        return jsonify(json.loads(json_util.dumps(document))), 200
    else:
        return jsonify({"error": "No vehicle found with the given vehicle number!"}), 400


if __name__ == '__main__':
    app.run(debug=True, port=8080)
