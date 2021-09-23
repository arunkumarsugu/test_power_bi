import requests
import pandas as pd
import os
from datetime import datetime as dt
from datetime import timedelta
import powerbi_config
import json


def get_authorization_token(user, password, env):
    auth_body = {
        'user_email': user,
        'password': password
    }
    auth_api = {
        'DEV': 'https://api.devtest.experience.com/v2/core/login',
        'PREPROD': 'https://api.preprod.experience.com/v2/core/login',
        'PROD': 'https://api.experience.com/v2/core/login'
    }
    access_response = requests.post(auth_api.get(env), data=auth_body)
    if access_response.status_code == 200:
        access_token = access_response.json()['auth_token']
    else:
        access_token = None
    return access_token


reports = {
    "/generate/survey/statistics/report": "surveystatistics"
}


def get_date_range(date):
    today_date = dt.now()
    end_date = dt.now().strftime("%Y-%m-%d")
    start_date = (today_date - timedelta(days=date)).strftime("%Y-%m-%d")
    return [start_date, end_date]


def convert_data_into_file(data, report):
    cur_date_time = ("{:%Y_%m_%d}".format(dt.now()))
    filename = f"{report}_{cur_date_time}.csv"
    account_dir = f'{powerbi_config.Base_dir}'
    if not os.path.exists(account_dir):
        os.mkdir(account_dir)
    path = f"{account_dir}/{report}"
    if not (os.path.exists(path)):
        os.mkdir(path)
    file_path = f"{path}/{filename}"
    if report in "surveystatistics":
        df = pd.DataFrame.from_dict(data)
        df.to_csv(file_path)


def get_reports_api(env):
    base_url_api = {
        'DEV': "https://reports.devtest.experience.com",
        'PREPROD': "https://reports.preprod.experience.com",
        "PROD": "https://reports.experience.com"
    }
    return base_url_api.get(env)


def get_data():
    period = get_date_range(powerbi_config.Date_range)
    base_url = get_reports_api(powerbi_config.env)
    access_token = get_authorization_token(powerbi_config.user_name, powerbi_config.password, powerbi_config.env)
    if access_token and base_url:
        for k, v in reports.items():
            URL = f"{base_url}{k}"
            if v in ("surveystatistics", "surveyresults", "reviewsmanagement"):
                par = {"account_id": powerbi_config.account_id, "report_format": "json", "range_period": json.dumps(period)}
            data = requests.get(url=URL, params=par, headers={"Authorization": access_token})
            data_json = data.json()
            convert_data_into_file(data_json, v)
    else:
        print("Unable to process your request")


if __name__ == "__main__":
    get_data()