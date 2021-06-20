import sqlite3
import base64
import json
from datetime import datetime
import logging
from time import sleep
import requests

logging.basicConfig(level=logging.INFO)


def create_connection(db_file):
    """ returns an sqlite file based db connection"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(e)
        raise


def request_url_analysis(site, key):
    """

    :param site:    site address to fetch analysis for
    :param key:     virus-total api key to use for requests
    :return:        response dict with
                        url data: virus-total url data
                        analysis_data: virus-total analysis
    """
    response = {}
    headers = {
        "x-apikey": key
    }
    payload = {
        "url": site
    }
    # submit analasis request
    r = requests.post('https://www.virustotal.com/api/v3/urls', data=payload, headers=headers)
    res_json = json.loads(r.content.decode('utf8'))
    analysis_id = res_json['data']['id']

    # get url data:
    url_id = base64.urlsafe_b64encode(site.encode()).decode().strip("=")
    r = requests.get(f"https://www.virustotal.com/api/v3/urls/{url_id}", headers=headers)
    res_json = json.loads(r.content.decode('utf8'))
    response['url_data'] = res_json

    # poll for analysis results
    r = requests.get('https://www.virustotal.com/api/v3/analyses/' + analysis_id, headers=headers)
    res_json = json.loads(r.content.decode('utf8'))

    while res_json['data']['attributes']['status'] == 'queued':
        sleep(10)
        r = requests.get('https://www.virustotal.com/api/v3/analyses/' + analysis_id, headers=headers)
        res_json = json.loads(r.content.decode('utf8'))

    response['analysis_data'] = res_json
    return response


def load_sql_script(path_to_sql_script):
    with open(path_to_sql_script) as script_file:
        return script_file.read()


def insert_to_db(db_cur, site, analysis, url_data, site_in_db):
    counts = {
        'malware': 0,
        'malicious': 0,
        'phishing': 0
    }
    classifications = {}

    results = analysis['data']['attributes']['results']
    for result_key in results:
        if results[result_key]['result'] not in counts:
            counts[results[result_key]['result']] = 1
        else:
            counts[results[result_key]['result']] += 1

    categories = url_data['data']['attributes']['categories']
    for category in categories.keys():
        if categories[category] not in classifications:
            classifications[categories[category]] = 1
        else:
            classifications[categories[category]] += 1

    site_risk = 'risk' if counts['malware'] > 1 or counts['malicious'] > 1 or counts['phishing'] > 1 else 'safe'
    if site_in_db:
        logging.info(f"updating data for {site} in db")
        db_cur.execute(f""" 
            UPDATE sites SET site_risk = '{site_risk}', 
                   counts = '{json.dumps(counts)}',                                             
                   classifications = '{json.dumps(classifications)}',
                   ingest_timestamp = current_timestamp
            WHERE url = '{site}' """
        )
    else:
        logging.info(f"insert data for {site} to db")
        db_cur.execute(f""" 
            INSERT INTO sites(url, 
                              site_risk, 
                              counts, 
                              classifications) 
                   VALUES('{site}',
                          '{site_risk}',
                          '{json.dumps(counts)}',
                          '{json.dumps(classifications)}')"""
        )


def get_data_freshness(db_cur, site):
    """
    checks site's data freshness in db
    :param db_cur: sqlite3 db cursor
    :param site: site to check
    :return: the amount of seconds passed since site was last updated in the db
    """
    query = f"""SELECT ingest_timestamp FROM sites WHERE url = '{site}'"""
    res = db_cur.execute(query)
    row = res.fetchone()
    if row:
        time_diff = datetime.utcnow() - datetime.fromisoformat(row[0])
        return time_diff.total_seconds()
    return None


def main():
    logging.info("loading config")
    with open('config/config.json') as config_file:
        config = json.load(config_file)

    logging.info("creating DB connection..")
    db_con = create_connection(config['db_location'])
    db_cur = db_con.cursor()

    logging.info("creating sites table in DB if needed..")
    db_cur.executescript(load_sql_script('sql/create_table.sql'))

    logging.info("reading ds1.csv from input folder for sites list..")
    with open(config['path_to_input_file']) as input_csv:
        input_ds = input_csv.readlines()

    for site in input_ds:
        site_in_db = False
        site = site.strip()
        # check if site's data is fresh in db
        data_freshness = get_data_freshness(db_cur, site)
        if data_freshness:
            site_in_db = True
            if data_freshness < 60 * config['data_freshness_threshold_minutes']:
                logging.info(f"data for {site} is {data_freshness} seconds old")
                continue
        logging.info(f"fetch {site} data from backend")
        data = request_url_analysis(site, config['virus_total_api_key'])
        insert_to_db(db_cur, site, data['analysis_data'], data['url_data'], site_in_db)
        db_con.commit()

    res = db_cur.execute(""" SELECT * FROM sites """)
    logging.info(res.fetchall())
    db_con.close()


if __name__ == '__main__':
    main()
