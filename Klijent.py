import asyncio
import aiohttp
import pandas as pd

# Proba s 10 za pocetak
ClientIDs  = list(range(1, 11))

print("\n Učitavam dataset...")
Dataset = pd.read_json("FakeDataset.json", lines = True)
print("\n Dataset uspješno učitan!")

ClientRows  = int(len(Dataset) / len(ClientIDs))

# Dict sa ID-jevima klijenata i njihovim kodom
Clients = {id:[] for id in ClientIDs}
for id, codes in Clients.items():
	StartingRow = (id - 1) * ClientRows 
	EndingRow = StartingRow + ClientRows 
	for index, row in Dataset.iloc[StartingRow + 1:EndingRow + 1].iterrows():
		codes.append(row.get("content"))

Tasks = []
Results = []

async def klijent():
	global Tasks, Results
	print("\n Saljem podatke...")
	async with aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl = False)) as session:
		for id, codes in Clients.items():
			Tasks.append(asyncio.create_task(session.get("http://127.0.0.1:8081/master", json = { "client": id, "codes": codes })))
		print("\n Podaci poslani! \n")
		Results = await asyncio.gather(*Tasks)
		Results = [await res.json() for res in Results]

asyncio.get_event_loop().run_until_complete(klijent())