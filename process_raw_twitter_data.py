import json
import os
from os import path

import pandas as pd
import pycountry
from babel import Locale
from babel import languages as ln
from dateutil.parser import parse


def process_data():
    directory_path = "./data"
    country_dict = get_country_dict()
    unable_to_recognize = 0
    recognized_tweets = 0
    rows_list = []
    for file in os.listdir(directory_path):
        if file.endswith(".json"):
            file_path = path.join(directory_path, file)
            with open(file_path) as json_file:
                json_string = json_file.read()
                json_data = json.loads(json_string)

            df_to_process = pd.DataFrame.from_dict(json_data["records"])
            for _, row in df_to_process.iterrows():
                date = get_date_from_row(row)
                location = get_location_from_row(row, country_dict)
                if date is None or location is None:
                    unable_to_recognize += 1
                    continue
                recognized_tweets += 1
                rows_list.append({"date": date, "place": location[0], "original place": location[1]})
    df = pd.DataFrame(rows_list)
    df.to_csv("data.csv")
    print(df.head())
    print(unable_to_recognize)
    print(recognized_tweets)
    print(recognized_tweets / (recognized_tweets + unable_to_recognize))


def get_date_from_row(row):
    if row["created_at"] is None:
        return None
    return parse(row["created_at"]).date()


def get_location_from_row(row, county_dict):
    if row["place"] is not None:
        country_code = row["place"]["country_code"]
        country = pycountry.countries.get(alpha_2=country_code)
        if country is not None:
            return country.name.lower(), row["place"]["country"]

    if row["user"] is not None and row["user"]["location"] is not None:
        location = row["user"]["location"]
        for key in county_dict.keys():
            if key in location.lower():
                return county_dict[key], location
        return None


def get_country_dict():
    country_dict = {}
    for country in pycountry.countries:
        info = ln.get_territory_language_info(country.alpha_2)
        country_dict[country.name.lower()] = country.name.lower()
        if hasattr(country, "official_name"):
            country_dict[country.official_name.lower()] = country.name.lower()
        for lang in info.keys():
            try:
                locale = Locale(lang, country.alpha_2)
                country_dict[locale.territories[country.alpha_2].lower()] = country.name.lower()
            except:
                pass
        country_dict["USA".lower()] = "United States of America".lower()
    return country_dict


if __name__ == '__main__':
    process_data()
