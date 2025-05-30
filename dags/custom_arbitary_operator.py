from airflow.sdk import dag, task
import pendulum
import requests
import json

SRC_URL = "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/globe/land_ocean/ytd/12/1880-2022.json"
now = pendulum.now()


@dag(start_date=now, schedule="@daily", catchup=False)
def etl():
    @task()
    def retrieve(src) -> dict:
        resp = requests.get(url=src)
        data = resp.json()
        print(f"Data is: {data}")
        return data["data"]

    @task()
    def to_fahrenheit(temps: dict[int, dict[str, float]]) -> dict[int, float]:
        ret: dict[int, float] = {}
        for year, info in temps.items():
            ret[year] = float(info["anomaly"]) * 1.8 + 32

        return ret

    @task()
    def load(fahrenheit: dict[int, float]):
        filename = "/tmp/fahrenheit.json"
        s = json.dumps(fahrenheit)
        f = open(filename, "w")
        f.write(s)
        f.close()

        return f"file:///{filename}"

    data = retrieve(SRC_URL)
    fahrenheit = to_fahrenheit(data)
    load(fahrenheit)


etl()
