import asyncio
import aiohttp
import pandas as pd
from aiohttp import web

routes = web.RouteTableDef()

#proba s 10 za pocetak
ClientIDs = list(range(1, 11))

print("\n Učitavam dataset...")
Dataset = pd.read_json("FakeDataset.json", lines = True)
print("\n Dataset uspješno učitan!")

ClientRows = int(len(Dataset)/len(ClientIDs))

Clients = {id:[] for id in ClientIDs}
for id, codes in Clients.items():
	StartingRow = (id - 1) * ClientRows
	EndingRow = StartingRow + ClientRows
	for index, row in Dataset.iloc[StartingRow + 1:EndingRow + 1].iterrows():
		codes.append(row.get("content"))

@routes.get("/")
async def function(request):
    return web.json_response({"status": "OK"}, status=200)
    
app = web.Application()
app.router.add_routes(routes)
web.run_app(app, port=8080)