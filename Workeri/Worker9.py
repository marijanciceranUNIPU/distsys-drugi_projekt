import asyncio
import json
import random
import re
import pandas as pd
from aiohttp import web

routes = web.RouteTableDef()

@routes.get("/worker")
async def worker(request):
    try:
        print("Request/task received.")
        ReqRandWait = random.random() * 0.2 + 0.1
        print(ReqRandWait)
        await asyncio.sleep(ReqRandWait)
        data = await request.json()
        
        Words = re.sub(r'[^\w\s]', '', data.get("data")).split()
        print("Broj riječi:", Words)
        
        Res = len(Words)
        print("Rezultat:", Res)
        
        ResRandWait = random.random() * 0.2 + 0.1
        print(ResRandWait)
        await asyncio.sleep(ResRandWait)
        
        return web.json_response({"Name": "Worker", "Status": "OK", "Broj riječi": Res}, status = 200)
    except Exception as e:
        return web.json_response({"Name": "Worker", "Error": str(e)}, status = 500)

app = web.Application()
app.router.add_routes(routes)
web.run_app(app, port = 8090)