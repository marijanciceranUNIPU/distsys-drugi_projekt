import asyncio
import json
import aiohttp
import random
import logging
import pandas as pd
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

SampleSize = 1000
MaxTasks = 10
RecievedReq, ReturnedRes = 0, 0
TasksSent, TasksCompleted = 0, 0

# Broj workera koji se koriste
UsedWorkers = random.randint(5, 10)
print("Broj workera iskorišten:", UsedWorkers)

Workers = {"WorkerId" + str(id): [] for id in range(1, UsedWorkers + 1)}
print("Workeri:", Workers)

routes = web.RouteTableDef()
@routes.get("/master")

async def master(request):
	try:
		global UsedWorkers, SampleSize, MaxTasks, Workers, TasksSent, TasksCompleted, RecievedReq, ReturnedRes

		# Status zaprimljenih zahjeva
		RecievedReq += 1
		logging.info(f"Novi zatjev zaprimljen. Total: {RecievedReq}/{MaxTasks}")
		data = await request.json()
		LengthCodes = len(data.get("codes"))

		# Dijeljenje kodova u 1000 redaka
		allCodes = '\n'.join(data.get("codes"))
		data["codes"] = [allCodes[i:i + SampleSize] for i in range(0, len(allCodes), SampleSize)]
  
		Tasks = []
		Results = []
		async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl = False)) as session:
			# Slanje taskova na obradu i status poslanih taskova
			Tasks = [asyncio.create_task(session.get(f"http://127.0.0.1:{8081/master + (i % UsedWorkers) + 1}/", json = {"id": data.get("client"), "data": code})) for i, code in enumerate(data.get("codes"))]
			TasksSent += len(Tasks)
			logging.info(f"{len(Tasks)} Taskova poslano, total: {TasksSent}")
			Results = await asyncio.gather(*Tasks)
			TasksCompleted += len(Results)
			logging.info(f"{len(Results)} Taskova završeno, total: {TasksCompleted}")
			Results = [await result.json() for result in Results]
			Results = [result.get("numberOfWords") for result in Results]
			
			# Dohvaćanje obrađenih taskova i status dovršenih taskova
			Results = await asyncio.gather(*Tasks)
			TasksCompleted += len(Results)
			logging.info(f"Another {len(Results)} more Tasks completed. Total: {TasksCompleted}")
			Results = [await result.json() for result in Results]
			Results = [result.get("numberOfWords") for result in Results]

		# Response status
		ReturnedRes += 1
		logging.info(f"{ReturnedRes}/{MaxTasks} zahtjeva poslano")
		
		return web.json_response({"Name": "Master", "Status": "OK", "Klijent": data.get("client"), "AvgBrojRijeci": round(sum(Results)/LengthCodes, 2)}, status = 200)
	except Exception as e:
		return web.json_response({"Name": "Master", "Error": str(e)}, status = 500)

app = web.Application()
app.router.add_routes(routes)
web.run_app(app, port=8081)