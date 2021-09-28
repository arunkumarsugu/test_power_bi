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


reports = {"/generate/survey/results/report": "surveyresults",
           "/generate/reviews/management/report": "reviewsmanagement",
           "/generate/survey/statistics/report": "surveystatistics",
           "/generate/hierarchy/details/report": "hierarchydetails",
           "/generate/publish/history/report": "publishistory",
           "/generate/verified/users/report": "verifiedusers",
           "/generate/nps/trend/report": "npstrend",
           "/generate/sms/delivery/statistics/report": "smsdelivery",
           "/survey/email/delivery/status/report": "surveyemail",
           "/generate/nps/report": "npsreport",
           "/generate/tier/ranking/report": "tierranking",
           "/generate/incomplete/surveys/report": "incompletesurvey"}

reports_with_error = {"/generate/digest/report": "digest",
                      "/generate/account/statistics/report": "accountstatistics"}


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
    elif report in "surveyresults":
        survey_results = data.get("survey_results")
        df = pd.DataFrame.from_dict(survey_results)
        df.to_csv(file_path)
    elif report in "reviewsmanagement":
        review_management = data.get("reviews_management_tier_details")
        df = pd.DataFrame.from_dict(review_management)
        df.to_csv(file_path)
    elif report in "publishistory":
        publish_history = data.get("agent_details")
        df = pd.DataFrame.from_dict(publish_history)
        df.to_csv(file_path)
    elif report in "hierarchydetails":
        hierarchy_details = data.get("hierarchy_user_details")
        df = pd.DataFrame.from_dict(hierarchy_details)
        df.to_csv(file_path)
    elif report in "verifiedusers":
        verified_users = data.get("verified_user_details")
        df = pd.DataFrame.from_dict(verified_users)
        df.to_csv(file_path)
    elif report in "npstrend":
        test_tier = data.get("loading test-Tier")
        df = pd.DataFrame.from_dict(test_tier)
        df.to_csv(file_path)
    elif report in "smsdelivery":
        sms_delivery = data.get("sms_delivery_statistics")
        df = pd.DataFrame.from_dict(sms_delivery)
        df.to_csv(file_path)
    elif report in "surveyemail":
        survey_email = data.get("survey_delivery_statistics")
        df = pd.DataFrame.from_dict(survey_email)
        df.to_csv(file_path)
    elif report in "npsreport":
        nps_report = data.get("COPY Default-Tier")
        df = pd.DataFrame.from_dict(nps_report)
        df.to_csv(file_path)
    elif report in "tierranking":
        tier_ranking = data.get("tier_ranking_details")
        df = pd.DataFrame.from_dict(tier_ranking)
        df.to_csv(file_path)
    elif report in "incompletesurvey":
        incomplete_survey = data.get("incomplete_survey_details")
        df = pd.DataFrame.from_dict(incomplete_survey)
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
                par = {"account_id": powerbi_config.account_id, "report_format": "json",
                       "range_period": json.dumps(period)}
            elif v in "hierarchydetails":
                par = {"report_name": "Hierarchy Details", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json",
                       "period": powerbi_config.period}
            elif v in "publishistory":
                par = {"report_name": "Publish History", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json",
                       "tier_data": [{"label": "All Tier", "value": powerbi_config.account_id}]}
            elif v in "verifiedusers":
                par = {"report_name": "Publish History", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json",
                       "tier_data": [{"label": "All Tier", "value": powerbi_config.account_id}]}
            elif v in "npstrend":
                par = {"report_name": "NPS Trend Report", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json",
                       "period": powerbi_config.period}
            elif v in "smsdelivery":
                par = {"report_name": "SMS Delivery Statistics", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json", "range_period": json.dumps(period),
                       "campaign_id": powerbi_config.campaign_id, "reports": "hierarchy",
                       "tier_data": [{"label": "All Tier", "value": powerbi_config.account_id}]}
            elif v in "surveyemail":
                par = {"report_name": "Survey Email Delivery Status Report", "account_id": powerbi_config.account_id,
                       "account_name": powerbi_config.accountname,
                       "action": "Download", "report_format": "json", "range_period": json.dumps(period),
                       "campaign_id": powerbi_config.campaign_id, "reports": "hierarchy",
                       "tier_data": [{"label": "All Tier", "value": powerbi_config.account_id}]}
            elif v in "npsreport":
                par = {"report_name": "NPS Report", "account_id": powerbi_config.account_id, "report_format": "json",
                       "period": "All Time", "account_name": powerbi_config.accountname}
            elif v in "tierranking":
                par = {"report_name": "Ranking Report - Tier", "account_id": powerbi_config.account_id,
                       "report_format": "json", "year": powerbi_config.year,
                       "account_name": powerbi_config.accountname, "month":powerbi_config.month}
            elif v in "incompletesurvey":
                par = {"account_id": powerbi_config.account_id,"campaign_id":powerbi_config.campaign_id,
                       "report_format": "json","range_period":json.dumps(period)}
            data = requests.get(url=URL, params=par, headers={"Authorization": access_token})
            data_json = data.json()
            convert_data_into_file(data_json, v)
    else:
        print("Unable to process your request")


if __name__ == "__main__":
    get_data()
