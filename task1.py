from datetime import datetime
from util import create_connection, request_analysis, request_url_id
import logging
logging.basicConfig(level=logging.DEBUG)

logging.info("creating DB connection..")
db_con = create_connection("sqlite.db")
db_cur = db_con.cursor()

logging.info("creating sites table in DB if needed..")
db_cur.execute(''' 
    CREATE TABLE IF NOT EXISTS sites (
    id integer PRIMARY KEY AUTOINCREMENT,
    url text NOT NULL,
    site_risk text,
    clean_count integer,
    unrated_count integer,
    classification text,
    ingest_timestamp timestamp DEFAULT current_timestamp);
    ''')

logging.info("reading ds1.csv from input folder for sites list..")
with open('input/ds1.csv') as input_csv:
    input_ds = input_csv.readlines()

for input_row in input_ds:
    site_in_db = False
    site = input_row.strip()
    logging.info(f"fetching data for: {site} from db")
    # check db for existing site
    res = db_cur.execute(f"select ingest_timestamp from sites where url = '{site}'")
    row = res.fetchone()
    if row:
        site_in_db = True
        ingest_timestamp = row[0]
        time_diff = datetime.utcnow() - datetime.fromisoformat(ingest_timestamp)
        if time_diff.total_seconds() < 60 * 30:
            logging.info(f"data for {site} is valid - not updating")
            # continue
    logging.info(f"updating data for {site}")
    # request info from virus total
    analysis_id = request_url_id(site)
    analysis = request_analysis(analysis_id)

    # populate the db
    counts = {
        'clean': 0,
        'unrated': 0,
        'malicious': 0,
        'phishing': 0,
        'malware': 0
    }
    results = analysis['data']['attributes']['results']
    for result_key in results:
        if results[result_key]['result'] in counts:
            counts[results[result_key]['result']] += 1

    site_risk = 'risk' if counts['malware'] > 1 or counts['malicious'] > 1 or counts['phishing'] > 1 else 'safe'
    if site_in_db:
        logging.info("updating data in db")
        db_cur.execute(''' UPDATE sites SET site_risk = '{}', 
                                            clean_count = '{}', 
                                            unrated_count = '{}',
                                            classification = '{}',
                                            ingest_timestamp = current_timestamp
                                        WHERE url = '{}' '''.
                       format(site_risk, counts['clean'], counts['unrated'], 'demo_category', site))
    else:
        logging.info("insert new data")
        db_cur.execute(""" INSERT INTO sites(url, site_risk, clean_count, unrated_count, classification) 
                           VALUES('{}','{}','{}','{}','{}')""".
                       format(site, site_risk, counts['clean'], counts['unrated'], 'demo_category'))
    db_con.commit()
res = db_cur.execute(""" SELECT * FROM sites """)
logging.info(res.fetchall())

db_cur.close()
