"""
Headless Site Navigation and File Download (Using Selenium) to S3

This example demonstrates using Selenium (via Firefox/GeckoDriver) to:
1) Log into a website w/ credentials stored in connection labeled 'selenium_conn_id'
2) Download a file (initiated on login)
3) Transform the CSV into JSON formatting
4) Append the current data to each record
5) Load the corresponding file into S3

To use this DAG, you will need to have the following installed:
[XVFB](https://www.x.org/archive/X11R7.6/doc/man/man1/Xvfb.1.xhtml)
[GeckoDriver](https://github.com/mozilla/geckodriver/releases/download)

selenium==3.11.0
xvfbwrapper==0.2.9
"""
from datetime import datetime, timedelta
import os
import boa
import csv
import json
import time
import logging

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from xvfbwrapper import Xvfb

from airflow import DAG
from airflow.models import Connection
from airflow.utils.db import provide_session

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import S3Hook


S3_CONN_ID = ''
S3_BUCKET = ''
S3_KEY = ''

date = '{{ ds }}'

default_args = {
    'start_date': datetime(2018, 2, 10, 0, 0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'selenium_extraction_to_s3',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


def imap_py(**kwargs):
    selenium_conn_id = kwargs.get('templates_dict', None).get('selenium_conn_id', None)
    filename = kwargs.get('templates_dict', None).get('filename', None)
    s3_conn_id = kwargs.get('templates_dict', None).get('s3_conn_id', None)
    s3_bucket = kwargs.get('templates_dict', None).get('s3_bucket', None)
    s3_key = kwargs.get('templates_dict', None).get('s3_key', None)
    date = kwargs.get('templates_dict', None).get('date', None)

    @provide_session
    def get_conn(conn_id, session=None):
        conn = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first()
        )
        return conn

    url = get_conn(selenium_conn_id).host
    email = get_conn(selenium_conn_id).user
    pwd = get_conn(selenium_conn_id).password

    vdisplay = Xvfb()
    vdisplay.start()
    caps = webdriver.DesiredCapabilities.FIREFOX
    caps["marionette"] = True

    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', "text/csv")

    logging.info('Profile set...')
    options = Options()
    options.set_headless(headless=True)
    logging.info('Options set...')
    logging.info('Initializing Driver...')
    driver = webdriver.Firefox(firefox_profile=profile,
                               firefox_options=options,
                               capabilities=caps)
    logging.info('Driver Intialized...')
    driver.get(url)
    logging.info('Authenticating...')
    elem = driver.find_element_by_id("email")
    elem.send_keys(email)
    elem = driver.find_element_by_id("password")
    elem.send_keys(pwd)
    elem.send_keys(Keys.RETURN)

    logging.info('Successfully authenticated.')

    sleep_time = 15

    logging.info('Downloading File....Sleeping for {} Seconds.'.format(str(sleep_time)))
    time.sleep(sleep_time)

    driver.close()
    vdisplay.stop()

    dest_s3 = S3Hook(s3_conn_id=s3_conn_id)

    os.chdir('/root/Downloads')

    csvfile = open(filename, 'r')

    output_json = 'file.json'

    with open(output_json, 'w') as jsonfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            row = dict((boa.constrict(k), v) for k, v in row.items())
            row['run_date'] = date
            json.dump(row, jsonfile)
            jsonfile.write('\n')

    dest_s3.load_file(
        filename=output_json,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    dest_s3.connection.close()


with dag:

    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    selenium = PythonOperator(
        task_id='selenium_retrieval_to_s3',
        python_callable=imap_py,
        templates_dict={"s3_conn_id": S3_CONN_ID,
                        "s3_bucket": S3_BUCKET,
                        "s3_key": S3_KEY,
                        "date": date},
        provide_context=True
    )

    kick_off_dag >> selenium
