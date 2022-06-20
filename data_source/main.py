from fastapi import FastAPI
import pandas as pd
import json
from random import randint

import asyncio


app = FastAPI()
counter = 0
lock = asyncio.Lock()

@app.get("/orders")
async def orders():
    global counter

    async with lock:
        n  = randint(3,10)
        orders_df = pd.read_csv("data/orders.csv")
        orders_df.drop("eval_set", inplace=True, axis=1)
        idx = list(range(counter, counter + n ))
        # print(orders_df.head().to_dict(orient='records'))
        r_dict = { i : j for i, j in enumerate(orders_df.iloc[idx].to_dict(orient='records'))}
        r_json = json.dumps(r_dict)
        counter += n

    return r_json



