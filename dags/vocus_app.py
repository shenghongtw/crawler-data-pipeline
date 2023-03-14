import os
import json
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

target_url = 'https://vocus.cc/user/624dc4b5fd89780001acdc89'

def read_data():
    file_dir = os.path.dirname(__file__)
    old_article_path = os.path.join(file_dir, '../data/vocus_crawler/article.json')
    with open(old_article_path, 'r', encoding='UTF-8') as old_article_json:
        old_article = json.load(old_article_json)
    return old_article

def crawl_articleID():
    new_article = []
    req = requests.get(target_url)
    soup = BeautifulSoup(req.text, "html.parser")
    finds = soup.find_all('div', class_='landscapeCard__CardWrapper-sc-6e418s-0 cQorNn articleCard')
    for find in finds:
        new_article.append(find.select_one("a").get("href"))
    return new_article

def check_duplicate(**context):
    file_dir = os.path.dirname(__file__)
    old_article_path = os.path.join(file_dir, '../data/vocus_crawler/article.json')
    old_articleID = context['task_instance'].xcom_pull(task_ids='read_data')
    crawl_articleID = context['task_instance'].xcom_pull(task_ids='crawl_articleID')
    with open(old_article_path, 'w', encoding='UTF-8') as old_article_json:
        json.dump(crawl_articleID, old_article_json)

    no_duplicateID = [articleID for articleID in crawl_articleID if articleID not in old_articleID]
    return no_duplicateID

def decide_what_to_do(**context):
    no_dulicateID = context['task_instance'].xcom_pull(task_ids='check_duplicate')
    if len(no_dulicateID) == 0:
        return 'do_nothing'
    else:
        return 'crawl_article_tag'

def crawl_article_tag(**context):
    file_dir = os.path.dirname(__file__)
    content_path = os.path.join(file_dir, '../data/vocus_crawler/content.txt')
    content = '今日標的：'
    new_articleID = context['task_instance'].xcom_pull(task_ids='check_duplicate')
    for articleID in new_articleID:
        req = requests.get(f'https://vocus.cc{articleID}')
        soup = BeautifulSoup(req.text, "html.parser")
        finds = soup.find('ul', class_='tagList__TagsList-sc-1xrqgsq-0 lcKdpG')
        finds = finds.find_all('a')
        for find in finds:
            print(find)
            content += find.text
            content += ', '
    with open(content_path, 'w') as cont:
        cont.write(content)

def get_token():
    file_dir = os.path.dirname(__file__)
    token_path = os.path.join(file_dir, '../data/vocus_crawler/credentials/slack.json')
    with open(token_path, 'r') as fp:
        token = json.load(fp)['token']
        return token

def get_content():
    file_dir = os.path.dirname(__file__)
    content_path = os.path.join(file_dir, '../data/vocus_crawler/content.txt')
    with open(content_path, 'r') as cont:
        content = cont.read()
    return content


default_args = {
    'owner': 'sheng',
    'start_date': datetime(2022, 6, 13, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'vocus_app',
    catchup=False,
    default_args=default_args,
    schedule_interval='30 8 * * 1,2,3,4,5'
) as dag:
    read_data = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
    )

    crawl_articleID = PythonOperator(
        task_id='crawl_articleID',
        python_callable=crawl_articleID,
    )

    check_duplicate = PythonOperator(
        task_id='check_duplicate',
        python_callable=check_duplicate,
        provide_context=True
    )

    decide_what_to_do = BranchPythonOperator(
        task_id='decide_what_to_do',
        python_callable=decide_what_to_do,
        provide_context=True
    )

    crawl_article_tag = PythonOperator(
        task_id='crawl_article_tag',
        python_callable=crawl_article_tag,
        provide_context=True
    )

    send_notification = SlackAPIPostOperator(
        task_id='send_notification',
        token=get_token(),
        channel='#vocus',
        text=get_content(),
        icon_url='http://airbnb.io/img/projects/airflow3.png'
    )

    do_nothing = DummyOperator(task_id='do_nothing')
    
    read_data >> crawl_articleID >> check_duplicate >> decide_what_to_do
    decide_what_to_do >> crawl_article_tag >> send_notification
    decide_what_to_do >> do_nothing
