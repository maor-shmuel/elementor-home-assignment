import sqlite3
from sqlite3 import Error
import requests
from time import sleep
import json


req_headers = {
    "authority": "www.virustotal.com",
    "method": "POST",
    "path": "/ui/urls",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-ianguage": "en-US,en;q=0.9,es;q=0.8",
    "accept-language": "en-US,en;q=0.9,fr;q=0.8,he;q=0.7",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://www.virustotal.com",
    "referer": "https://www.virustotal.com/old-browsers/",
    "sec-ch-ua": '" Not;A Brand";v="99", "Microsoft Edge";v="91", "Chromium";v="91"',
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36 Edg/91.0.864.41",
    "x-requested-with": "XMLHttpRequest",
    "x-session-hash": "undefined",
    "x-tool": "oldbrowsers",
    "x-vt-anti-abuse-header": "N905SXN9DG8TT2JKWY46-ZG9udCBiZSBldmls-1623824042"
}


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
        raise


def request_url_id(site):
    url = 'https://www.virustotal.com/ui/urls'
    payload = {
        "url": site
    }
    r = requests.post(url, data=payload, headers=req_headers)
    res_json = r.content.decode('utf8').replace("'", '"')
    return json.loads(res_json)['data']['id']


def request_analysis(analysis_id):
    url = 'https://www.virustotal.com/ui/analyses' + '/' + analysis_id

    r = requests.get(url, headers=req_headers)
    res_json = json.loads(r.content.decode('utf8').replace("'", '"'))
    while res_json['data']['attributes']['status'] == 'queued':
        sleep(10)
        r = requests.get(url, headers=req_headers)
        res_json = json.loads(r.content.decode('utf8').replace("'", '"'))
        print(res_json['data']['attributes']['status'])

    return res_json

