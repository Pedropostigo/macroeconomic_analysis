import os

import urllib
import json
import zipfile

from datetime import date
import calendar

import pandas as pd

from datawarehouse import DataWarehouse

class EurGDPData():

    def __init__():
        pass

    @staticmethod
    def download_data(path:str):
        url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/teina010?format=JSON&unit=MIO_EUR_SCA&na_item=B1GQ&s_adj=SCA&lang=en"
        response = urllib.request.urlopen(url)

        response = json.loads(response.read())

        with zipfile.ZipFile(os.path.join(path, 'gdp_eur.zip'), "w") as zip_f:
            zip_f.writestr("data.json", data = json.dumps(response))


    @staticmethod
    def process_data(origin_path:str, destination_path:str,
                     output_file_type:str = "parquet"):

        with zipfile.ZipFile(os.path.join(origin_path, "gdp_eur.zip"), "r") as zip_f:
            json_gdp_data = zip_f.read("data.json")
            json_gdp_data = json.loads(json_gdp_data)

        records = []

        # geo dimensions
        dimension_geo = json_gdp_data['dimension']['geo']['category']['index']
        dimendion_geo_label = json_gdp_data['dimension']['geo']['category']['label']

        # time dimensions
        dimension_time = json_gdp_data['dimension']['time']['category']['index']
        dimendion_time_label = json_gdp_data['dimension']['time']['category']['label']

        # process data
        counter = 0
        for g in dimension_geo:
            for t in dimension_time:
                key = str(counter)

                value = json_gdp_data['value'].get(key)
                status = json_gdp_data['status'].get(key)

                geo_label = dimendion_geo_label[g]
                time_label = dimendion_time_label[t]

                records.append(
                    {
                        'geo_id': g,
                        'geo_des': geo_label,
                        'time_des': time_label, 
                        'value': value,
                        'status': status
                    }
                )
                counter +=1

        if output_file_type == "csv":
            pd.DataFrame.from_records(records).to_csv(
                os.path.join(os.path.join(destination_path, "gdp_eur.csv")),
                index = False
            )
        elif output_file_type == "parquet":
            pd.DataFrame.from_records(records).to_csv(
                os.path.join(os.path.join(destination_path, "gdp_eur.parquet")),
                index = False
            )

    @staticmethod
    def _quarter_to_date(quarter_str: str) -> str:
        quarter_str_split = quarter_str.split("-")
        
        year = int(quarter_str_split[0])
        quarter = int(quarter_str_split[1].replace("Q", ""))

        month = quarter * 3
        day = calendar.monthrange(year, month)[1]

        date_str = date(year, month, day).strftime("%Y-%m-%d")
        return date_str

    @staticmethod
    def ingest_data(origin_path:str, data_warehouse: DataWarehouse) -> None:
        data = pd.read_csv(origin_path)
        data = data.values.tolist()
        print(data)

if __name__ == "__main__":
    quarter_str = "2023-Q1"
    print(EurGDPData._quarter_to_date(quarter_str))