import asyncio
import json
import aiohttp
import random
import logging
import pandas as pd
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

SampleSize = 1000
MaxTasks = 10000
RecievedReq, ReturnedRes = 0, 0
TasksSent, TasksCompleted = 0, 0

# Broj workera koji se koriste
UsedWorkers = random.randint(5, 10)
print("Broj workera iskorišten:", UsedWorkers)

Workers = {"WorkerId" + str(id): [] for id in range(1, UsedWorkers + 1)}
print("Workeri:", Workers)

routes = web.RouteTableDef()
@routes.get("/")

async def master(request):
    try:
        global UsedWorkers, SampleSize, MaxTasks, Workers, TasksSent, TasksCompleted, RecievedReq, ReturnedRes

        # Status zaprimljenih zahjeva
        RecievedReq += 1
        logging.info(f"Novi zatjev zaprimljen. Total: {RecievedReq}/{MaxTasks}")
        if(RecievedReq == 10000):
            print("\n GOTOVO! \n")
        Data = await request.json()
        LengthCodes = len(Data.get("codes"))

        # Dijeljenje kodova u 1000 redaka
        allCodes = '\n'.join(Data.get("codes"))
        codes = allCodes.split("\n")
        Data["codes"] = ["\n".join(codes[i:i+SampleSize]) for i in range(0, len(codes), SampleSize)]

        Tasks = []
        Results = []
        async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl = False)) as session:
            # Slanje taskova na obradu i status poslanih taskova
            Tasks = [
                asyncio.create_task(
                    session.get(f"http://127.0.0.1:{8081 + currentWorker}/", json = { "id": Data.get("client"), "data": code_chunk })
                ) for currentWorker, code_chunk in enumerate(Data.get("codes"), start=1) if currentWorker <= UsedWorkers
            ]
        TasksSent += len(Tasks)
        logging.info(f"Poslano {len(Tasks)} taskova. Total taskova poslano: {TasksSent}")
        
        # Dohvaćanje obrađenih taskova i status dovršenih taskova
        Results = await asyncio.gather(*Tasks)
        TasksCompleted += len(Results)
        logging.info(f"Dodatnih {len(Results)} taskova završno. Total taskova završeno: {TasksCompleted}")
        Results = [await result.json() for result in Results]
        Results = [result.get("numberOfWords") for result in Results]

        # Response status
        ReturnedRes += 1
        logging.info(f"{ReturnedRes}/{MaxTasks} zahtjeva poslano")

        return web.json_response({"Name": "Master", "Status": "OK", "Client": Data.get("client"), "AvgNumWords": round(sum(Results) / LengthCodes, 2)}, status = 200)
    except Exception as e:
        return web.json_response({"Name": "Master", "Error": str(e)}, status = 500)

app = web.Application()
app.router.add_routes(routes)
web.run_app(app, port=8081, access_log=None)