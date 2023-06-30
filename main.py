import pandas as pd
import requests
from etl import Etl

url = "https://jsonplaceholder.typicode.com/albums"


def get_data():
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()

        return pd.DataFrame(json_data)
    else:
        print("Error al obtener el JSON: ", response.status_code)


if __name__ == "__main__":
    df = get_data()
    etl = Etl(pandas_df=df, table='albums')
    etl.execute()
