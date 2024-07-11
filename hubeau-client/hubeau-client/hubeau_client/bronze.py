import requests
from google.cloud import bigquery

import pandas

def extract():
    print("call api")
    uri = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_entite=O972001001"
    data = requests.get(uri).json()["data"]
    df = pandas.DataFrame(data)
    print(df)
    return df

def load(data):
    print("run queries", data)

def etl():
    data = extract()
    load(data)

if __name__ == "__main__":
    etl()
