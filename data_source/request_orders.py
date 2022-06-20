import requests
import pandas as pd

result = requests.get("http://localhost:80/orders")
orders = pd.read_json(result.json()).transpose()
print(orders)
