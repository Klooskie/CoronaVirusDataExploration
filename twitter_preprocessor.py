import json
import os
import sys
from os import path

import pandas as pd
import pycountry
from babel import Locale
from babel import languages as ln
from dateutil.parser import parse


def process_data(args):
    if len(args) != 3:
        print("2 arguments must be passed to the script:\n"
              "1st - the directory path in which are twitter data stored;\n"
              "2nd - the location of the output file.\n")
        return
    directory_path = args[1]
    result_file_location = args[2]
    country_dict = get_country_dict()
    unable_to_recognize = 0
    recognized_tweets = 0
    result_dict = {}
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

                add_to_result(result_dict, date, location)
                recognized_tweets += 1
            print(f"File {file} preprocessed successfully!")

    save_data(result_dict, result_file_location)

    print('Count of tweets with unrecognizable date or location:', unable_to_recognize)
    print('Count of properly processed tweets:', recognized_tweets)
    print('Percentage of recognized tweets:', recognized_tweets / (recognized_tweets + unable_to_recognize))


def get_date_from_row(row):
    if row["created_at"] is None:
        return None
    return parse(row["created_at"]).date()


def get_location_from_row(row, county_dict):
    if row["place"] is not None:
        country_code = row["place"]["country_code"]
        country = pycountry.countries.get(alpha_2=country_code)
        if country is not None:
            return country.name.lower()

    if row["user"] is not None and row["user"]["location"] is not None:
        location = row["user"]["location"]
        for key in county_dict.keys():
            if key in location.lower():
                return county_dict[key]


def add_to_result(result_dict, date, location):
    if date not in result_dict:
        result_dict[date] = dict((country.name.lower(), 0) for country in pycountry.countries)
    result_dict[date][location] += 1


def save_data(result_dict, location):
    for day in result_dict.keys():
        file_path = path.join(location, day.strftime("%d-%m-%Y.csv"))
        rows_list = []
        data = result_dict[day]
        for country in data.keys():
            rows_list.append({"country": country, "tweets": data[country]})
        df = pd.DataFrame(rows_list)
        if path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            df = pd.concat([df, existing_df]).groupby('country')['tweets'].sum().reset_index()
            df.to_csv(file_path)
        else:
            df.to_csv(file_path)


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
    country_dict["USA".lower()] = "United States".lower()
    return country_dict


if __name__ == '__main__':
    process_data(sys.argv)
