import cleaner.data_cleaner as cl
from collections import defaultdict, Counter
import json
import os
import pytest


@pytest.fixture()
def removeTmpFile():
    fname = '/tmp/cleaned_data_2018-01-16.json'
    if os.path.isfile(fname):
        os.remove('/tmp/cleaned_data_2018-01-16.json')


@pytest.mark.usefixtures("removeTmpFile")
def test_count_after_cleaning():
    dc = cl.DataCleaner()
    score = Counter()
    date = set()
    names = set()
    classes = set()
    output_files = set()
    unique_events = set()
    unique_users = set()
    event_count = Counter()
    users = defaultdict(lambda: {'history_time': [], 'first_names': Counter(), 'names': 0, 'score_count': 0, 'event_count': 0, 'identify_calls': 0, 'launch_events': 0})
    file_name = 'tests/ressources/test_file_users.json'
    cnt = dc.clean_user(file_name, score, date, users, names, classes, output_files, unique_events, unique_users, event_count)
    assert cnt == 22


@pytest.mark.usefixtures("removeTmpFile")
def test_different_user_ids_after_cleaning():
    dc = cl.DataCleaner()
    score = Counter()
    date = set()
    names = set()
    classes = set()
    output_files = set()
    unique_events = set()
    unique_users = set()
    event_count = Counter()
    users = defaultdict(lambda: {'history_time': [], 'first_names': Counter(), 'names': 0, 'score_count': 0, 'event_count': 0, 'identify_calls': 0, 'launch_events': 0})
    file_name = 'tests/ressources/test_file_users.json'
    cnt = dc.clean_user(file_name, score, date, users, names, classes, output_files, unique_events, unique_users, event_count)
    assert cnt == 22
    user_cleaned = []
    with open('/tmp/cleaned_data_2018-01-16.json') as fp:
        for count_user_cleaned, row in enumerate(fp):
            user_cleaned.append(json.loads(row))

    assert user_cleaned[1]['user_id'] != user_cleaned[4]['user_id']
    assert user_cleaned[4]['user_id'] != user_cleaned[9]['user_id']
    assert user_cleaned[9]['user_id'] != user_cleaned[16]['user_id']
    assert user_cleaned[9]['user_id'] != user_cleaned[22]['user_id']


@pytest.mark.usefixtures("removeTmpFile")
def test_event_orders_with_user_ids():
    dc = cl.DataCleaner()
    score = Counter()
    date = set()
    names = set()
    classes = set()
    output_files = set()
    unique_events = set()
    unique_users = set()
    event_count = Counter()
    users = defaultdict(lambda: {'history_time': [], 'first_names': Counter(), 'names': 0, 'score_count': 0, 'event_count': 0, 'identify_calls': 0, 'launch_events': 0})
    file_name = 'tests/ressources/test_file_users.json'

    cnt = dc.clean_user(file_name, score, date, users, names, classes, output_files, unique_events, unique_users, event_count)
    assert cnt == 22
    user_cleaned = []
    with open('/tmp/cleaned_data_2018-01-16.json') as fp:
        for count_user_cleaned, row in enumerate(fp):
            user_cleaned.append(json.loads(row))

    assert user_cleaned[1]['user_id'] == user_cleaned[3]['user_id']
    assert user_cleaned[1]['user_id'] == user_cleaned[3]['user_id']
    assert user_cleaned[1]['user_id'] == user_cleaned[3]['user_id']

    assert user_cleaned[4]['user_id'] == user_cleaned[5]['user_id']
    assert user_cleaned[4]['user_id'] == user_cleaned[6]['user_id']
    assert user_cleaned[4]['user_id'] == user_cleaned[7]['user_id']
    assert user_cleaned[4]['user_id'] == user_cleaned[8]['user_id']

    assert user_cleaned[9]['user_id'] == user_cleaned[10]['user_id']
    assert user_cleaned[9]['user_id'] == user_cleaned[11]['user_id']
    assert user_cleaned[9]['user_id'] == user_cleaned[12]['user_id']
    assert user_cleaned[9]['user_id'] == user_cleaned[13]['user_id']
    assert user_cleaned[9]['user_id'] == user_cleaned[14]['user_id']
    assert user_cleaned[9]['user_id'] == user_cleaned[15]['user_id']

    assert user_cleaned[16]['user_id'] == user_cleaned[17]['user_id']
    assert user_cleaned[16]['user_id'] == user_cleaned[18]['user_id']

    assert user_cleaned[19]['user_id'] == user_cleaned[20]['user_id']
    assert user_cleaned[19]['user_id'] == user_cleaned[21]['user_id']
    assert user_cleaned[19]['user_id'] == user_cleaned[22]['user_id']
