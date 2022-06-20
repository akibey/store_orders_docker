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
        n = randint(3,10)
        orders_df = pd.read_csv("/code/app/data/fin_orders_joined.csv")
        max_nid = orders_df["id"].nunique()
        idx = list(range(counter, counter + n ))
        ids = orders_df["id"].sort_values().unique()[idx]
        orders_sel= orders_df[orders_df['id'].isin(ids)].reset_index(drop=True)
        # print(orders_df.head().to_dict(orient='records'))
        # r_dict = { i : j for i, j in enumerate(orders_df.iloc[idx].to_dict(orient='records'))}
        r_dict = { i : j for i, j in enumerate(orders_sel.to_dict(orient='records'))}

        r_json = json.dumps(r_dict)
        counter += n
        if counter >= max_nid:
            counter = 0
    return r_json



