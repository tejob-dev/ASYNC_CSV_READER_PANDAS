from flask import make_response
from quart import Quart, request, jsonify
from flask_restful import Resource, Api
import sys
import os
import pandas as pd
import time
from unidecode import unidecode
from concurrent.futures import ThreadPoolExecutor
# from celery import Celery
import asyncio
import uuid

app = Quart(__name__)
# api = Api(app)
port = 5585

task_states = {}
executor = ThreadPoolExecutor(max_workers=4)

if sys.argv.__len__() > 1:
    port = sys.argv[1]
print("You said port is : {} ".format(port))

# class HelloWorld(Resource):
@app.route('/', methods=['GET'])
def get():
    return {'hello': 'world Port : '+port}

# api.add_resource(HelloWorld, '/')

def remove_accents_and_uppercase(input_str):
    # Remove accents
    no_accents = unidecode(str(input_str).strip())
    # Convert to uppercase
    uppercase_str = no_accents.upper()
    # print(uppercase_str)
    return uppercase_str

def check_elector_sync(data, task_id):
    try:
        datacsv = pd.read_csv(
            "./data.csv",
            usecols=["Numero Electeur" ,"Nom/Nom de Jeune Fille", "Prenoms", "Date de Naissance"],
            chunksize=250000,
            delimiter=",",
            encoding="latin-1"
        )
        iselectoratfind = False
        thereelectorcard = None
        chunkidx = 0

        if data and data['nom'] and data['prenom'] and data['date_naiss']:
            for idx, chunk in enumerate(datacsv):
                if iselectoratfind:
                    break
                df = pd.DataFrame(chunk)
                chunkidx = idx
                for i in range(len(df)):
                    if (
                        remove_accents_and_uppercase(df.loc[i, "Nom/Nom de Jeune Fille"]) == remove_accents_and_uppercase(data["nom"]) and
                        remove_accents_and_uppercase(df.loc[i, "Prenoms"]) == remove_accents_and_uppercase(data["prenom"]) and
                        df.loc[i, "Date de Naissance"] == data["date_naiss"]
                    ):
                        iselectoratfind = True
                        thereelectorcard = df.loc[i, "Numero Electeur"]
                        break

                task_states[task_id]["progress"] = int((idx + 1) / len(list(datacsv)) * 100)

        task_states[task_id] = {
            "state": "COMPLETED",
            "result": {"data": iselectoratfind, "cardelect":thereelectorcard, "status": 200, "chunk": chunkidx, "exec_time": time.time()}
        }
    except Exception as e:
        task_states[task_id] = {
            "state": "FAILED",
            "error": str(e)
        }

@app.route('/check_elector', methods=['POST'])
async def check_elector():
    data = await request.get_json()
    task_id = str(uuid.uuid4())
    task_states[task_id] = {"state": "PENDING", "progress": 0}

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, check_elector_sync, data, task_id)

    return jsonify({"task_id": task_id}), 202

@app.route('/check_elector_status/<task_id>', methods=['GET'])
async def check_elector_status(task_id):
    if task_id not in task_states:
        return jsonify({"error": "Invalid task ID"}), 404

    if task_states[task_id]["state"] == "PENDING":
        return jsonify(task_states[task_id]), 302

    return jsonify(task_states[task_id]), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=port)