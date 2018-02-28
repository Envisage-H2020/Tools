# coding: utf-8
'''Code cleaning Wind Energy Lab Data

Usage:
    data_cleaner.py clean <input_file>
'''
import json
import pandas as pd
from collections import defaultdict, Counter
import datetime
import hashlib
from docopt import docopt
import random


class DataCleaner(object):

    class_mapping = {"εα":"EA",
                    "elinigerminiki agogi":"EA",
                    "Ελληνογερμανικη αγωγη":"EA",
                    "ea":"EA",
                    "Ελληνογερμανικη Αγωγη":"EA",
                    "elinogerminiki agogi":"EA",
                    "Ε.Α.":"EA",
                    "EA":"EA",
                    "elinogermaniki agogi":"EA",
                    "ΕΑΑ":"EA",
                    "ΑΕ":"EA",
                    "ΕΛ":"EA",
                    "ΕΑ":"EA",
                    "Εα":"EA",
                    "ΣΤ'2":"ΣΤ2",
                    "Στ":"ΣΤ",
                    "στ1":"ΣΤ1",
                    "στ'1":"ΣΤ1",
                    "ΣΤ10":"ΣΤ1",
                    "ΣΤ1":"ΣΤ1",
                    "στ1":"ΣΤ1",
                    "ΣΤ'1":"ΣΤ1",
                    "στ2":"ΣΤ2",
                    "στ΄2":"ΣΤ2",
                    "ST2":"ΣΤ2",
                    "st2":"ΣΤ2",
                    "ΣΤ2":"ΣΤ2",
                    "ΣΤ΄2":"ΣΤ2",
                    "στ3":"ΣΤ3",
                    "ΣΤ33":"ΣΤ3",
                    "ΣΤ3":"ΣΤ3",
                    "στ 3":"ΣΤ3",
                    "στ4":"ΣΤ4",
                    "ΣΤ44":"ΣΤ4",
                    "ΣΤ4":"ΣΤ4",
                    "ΣΤ΄4":"ΣΤ4",
                    "ΣΤ'4":"ΣΤ4",
                    "Στ 5":"ΣΤ5",
                    "στ5":"ΣΤ5",
                    "στ΄5":"ΣΤ5",
                    "Στ5":"ΣΤ5",
                    "στ'5":"ΣΤ5",
                    "st5":"ΣΤ5",
                    "ST5":"ΣΤ5",
                    "στ'5":"ΣΤ5",
                    "ΣΤ5":"ΣΤ5",
                    "στ6":"ΣΤ6",
                    "ΣΤ΄6":"ΣΤ6",
                    "Στ6":"ΣΤ6",
                    "ΣΤ6":"ΣΤ6",
                    "ΣΤ'6":"ΣΤ6",
                    "στ7":"ΣΤ7",
                    "st7":"ΣΤ7",
                    "ST7":"ΣΤ7",
                    "ΣΤ7":"ΣΤ7",
                    "ΣΤ΄7":"ΣΤ7",
                    "Στ'7":"ΣΤ7",
                    "Στ7":"ΣΤ7",
                    "Στ'7":"ΣΤ7"}

    def create_user_id(self, user_id):
        user_id_temp = "%s%d" % (user_id, random.randint(1, 10000000))
        return hashlib.sha1(user_id_temp.encode('utf-8')).hexdigest()

    def clean_user(self, file_name, score, date, users, names, classes, output_files, unique_events, unique_users, event_count):
        # rows = sorted(map(lambda x: json.loads(x), rows), key=lambda x: (x['user_id'], x['ts']))
        # this is only working if there is at least one user
        current_user_id = None
        print (file_name)
        with open(file_name) as fp:
            for count_user_cleaned, row in enumerate(fp):
                try:
                    row = json.loads(row)
                    if not current_user_id:
                        current_user_id = row['user_id']
                    users[row['user_id']]['event_count'] += 1
                    if row['user_id'] not in users:
                        users[row['user_id']]['history_time'].append(row['ts'])
                    if row['event'] == 'launch':
                        users[row['user_id']]['launch_events'] += 1
                    elif row['event'] == 'group' and 'event_value' in row:
                        if row['event_value'] in self.class_mapping:
                            row['event_value'] = self.class_mapping[row['event_value']]
                        if 'class' in row['event_id']:
                            classes.add(row['event_value'])
                        elif 'school' in row['event_id']:
                            schools.add(row['event_value'])
                    elif row['event'] == 'identify':
                        users[row['user_id']]['identify_calls'] += 1
                        if 'first_name' in row:
                            row['first_name'] = row['first_name'].upper()
                            first_name = row['first_name']
                        names.add(first_name)
                        users[row['user_id']]['names'] += 1
                        users[row['user_id']]['first_names'][first_name] += 1
                        current_user_id = self.create_user_id(row['user_id'])
                    elif row['event'] == 'game.result' and row['event_id'] == 'score':
                        score[int(row['event_value'])] += 1
                        user.add(row['user_id'])
                        users[row['user_id']]['score_count'] += 1
                    unique_events.add(row['event'])
                    unique_users.add(row['user_id'])
                    event_count[row['event']] += 1
                    date.add(datetime.datetime.fromtimestamp(int(row['ts'])).strftime('%Y-%m-%d'))
                    users[row['user_id']]['history_time'].append(row['ts'])
                    row['user_id'] = current_user_id
                    # prevent to much memory consumption
                    output_file = '/tmp/cleaned_data_%s.json' % "-".join(date)
                    output_files.add(output_file)
                    with open(output_file, 'ab') as fp:
                        fp.write("%s\n" % json.dumps(row))
                except Exception as e:
                    print ("Somthing went wrong while opening the file: %s" % e)
                    print ("Wrong filename?: %s" % file_name)
                    print ("Error occured at line %d" % count_user_cleaned)
        return count_user_cleaned

if __name__ == '__main__':
    arguments = docopt(__doc__)
    print (arguments)
    if arguments['<input_file>']:
        file_name = arguments['<input_file>']
    else:
        print ("You need to add a file")
        exit(0)

    score = Counter()
    user = set()
    date = set()
    output_files = set()
    names = set()
    unique_events = set()
    unique_users = set()
    event_count = Counter()
    classes = set()
    schools = set()
    user_cleaned = []
    users = defaultdict(lambda: {'history_time': [], 'first_names': Counter(), 'names': 0, 'score_count': 0, 'event_count': 0, 'identify_calls': 0, 'launch_events': 0})
    user_stats = defaultdict(lambda: {'duration': 0, 'names': 0, 'first_names': defaultdict(), 'score': 0, 'event_count': 0, 'identify_calls': 0, 'launch_events': 0})
    user_cleaned = []

    dc = DataCleaner()
    count_user_cleaned = dc.clean_user(file_name, score, date, users, names, classes, output_files, unique_events, unique_users, event_count)
    # write cleaned users to new jsonp file

    print ('Date Interval')
    print (date)
    print ('Count Rows Cleaned')
    print (count_user_cleaned)
    # print (names)
    print ("Count Classes: %d" % len(classes))
    print ("Count Schools: %d" % len(schools))
    print ("Count Names: %d" % len(names))
    print ("#############################")
    print ("Speed Stats\n")
    print ("User Count")
    print (len(unique_users))
    print ("Unique Events")
    print (event_count)
    print (len(event_count))

    print ("Event Count")
    print (len(unique_events))

    # stats can be considered to print for more detailed informations
    for s in user:
        min_ts = min(users[s]['history_time'])
        max_ts = max(users[s]['history_time'])
        duration = max_ts - min_ts
        user_stats[s]['duration'] = duration/60
        user_stats[s]['score'] = users[s]['score_count']
        user_stats[s]['event_count'] = users[s]['event_count']
        user_stats[s]['identify_calls'] = users[s]['identify_calls']
        user_stats[s]['launch_events'] = users[s]['launch_events']
        user_stats[s]['names'] = users[s]['names']
        user_stats[s]['first_names'] = users[s]['first_names']

    df_users = pd.DataFrame.from_dict(user_stats, orient='index').reset_index()
    df_users.to_csv("\tmp\users_stats_tmp_cleaned.csv", sep='\t', encoding='utf-8')
    df_users = (df_users.rename(columns={'index': 'user_id'}))
    print("Identify: %d" % df_users['identify_calls'].sum())
    print("Score: %d" % df_users['identify_calls'].sum())
    print ("Cleaned data is stored in: %s" % str(output_files))
