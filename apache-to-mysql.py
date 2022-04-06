#!/usr/bin/env python3

import sys
import mysql.connector
import apache_log_parser
# import json
from datetime import datetime
import yaml

if len(sys.argv) != 2:
    print("Usage:", sys.argv[0], "/var/log/apache2/access.log")
    exit(1)

# Open config file
with open("config.yml", "r") as yamlfile:
    data = yaml.load(yamlfile, Loader=yaml.FullLoader)

# Establish the database connection
conn = mysql.connector.connect(user=data['database']['username'], password=data['database']['password'], host=data['database']['host'], database=data['database']['catalog'])

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS logs (
#                     status INTEGER,
#                     request_method TEXT,
#                     request_url TEXT,
#                     date TEXT
#                 )
#             """)

# Pattern below is from the LogFormat setting in apache2.conf/httpd.conf file
# You will likely need to change this value to the pattern your system uses
parser = apache_log_parser.make_parser(data['logFormat'])

log_file = sys.argv[1]

counter = 0
printFirstLine = data['printfirstline']
executeSql = data['executeSql']

# now = datetime.now()
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " Starting")

conn.start_transaction()

with open(log_file) as f:

    for line in f:

        d = parser(line)
        # print(d)

        # Line below adds minimalistic date stamp column
        # in format that sqlite3 date functions can work with
        # d['date'] = d['time_received_datetimeobj'].date().isoformat()
        d['date'] = d['time_received_datetimeobj'].strftime("%Y-%m-%d %H:%M:%S")
        d['date_utc'] = d['time_received_utc_datetimeobj'].strftime("%Y-%m-%d %H:%M:%S")

        record = (
            d['remote_host'], d['remote_logname'], d['remote_user'],
            d['date'], d['date_utc'], d['request_method'], d['request_url'],
            d['request_http_ver'], d['status'], d['bytes_tx'],
            d['request_header_referer'], d['request_header_user_agent'], d['request_header_user_agent__browser__family'],
            d['request_header_user_agent__browser__version_string'], d['request_header_user_agent__os__family'],
            d['request_header_user_agent__os__version_string'], d['request_header_user_agent__is_mobile']
        )

        # cursor.execute("""
        #                 INSERT INTO logs ( `remote_host`, `remote_logname`, `remote_user`,`date`, `date_utc`,
        #                                     `request_method`, `request_url`, `request_http_ver`, `status`, `bytes_tx`,
        #                                     `request_header_referer`, `user_agent`, `user_agent_br_family`,
        #                                     `user_agent_br_version`, `user_agent_os_family`,
        #                                     `user_agent_os_version`, `user_agent_is_mobile`)
        #                           VALUES (:remote_host, :remote_logname, :remote_user,
        #                             :date, :date_utc, :request_method, :request_url,
        #                             :request_http_ver, :status, :bytes_tx,
        #                             :request_header_referer, :request_header_user_agent, :request_header_user_agent__browser__family,
        #                             :request_header_user_agent__browser__version_string, :request_header_user_agent__os__family,
        #                             :request_header_user_agent__os__version_string, :request_header_user_agent__is_mobile)
        #                 """, d)

        # cursor.execute("""
        #                 INSERT INTO `access` ( `remote_host`, `remote_logname`, `remote_user`,`date`, `date_utc`,
        #                                     `request_method`, `request_url`, `request_http_ver`, `status`, `bytes_tx`,
        #                                     `request_header_referer`, `user_agent`, `user_agent_br_family`,
        #                                     `user_agent_br_version`, `user_agent_os_family`,
        #                                     `user_agent_os_version`, `user_agent_is_mobile`)
        #                           VALUES ('test', 'test', 'test', 'test', 'test',
        #                                     'test', 'test', 'test', 'test', 'test',
        #                                     'test', 'test', 'test',
        #                                     'test', 'test',
        #                                     'test', 'test')
        #                 """, d)

        # e = tuple(d)
        # cursor.execute("""
        #                 INSERT INTO `access` ( `remote_host`, `remote_logname`, `remote_user`,`date`, `date_utc`,
        #                                     `request_method`, `request_url`, `request_http_ver`, `status`, `bytes_tx`,
        #                                     `request_header_referer`, `user_agent`, `user_agent_br_family`,
        #                                     `user_agent_br_version`, `user_agent_os_family`,
        #                                     `user_agent_os_version`, `user_agent_is_mobile`)
        #                           VALUES (%s, %s, %s, %s, %s,
        #                                     %s, %s, %s, %s, %s,
        #                                     %s, %s, %s,
        #                                     %s, %s,
        #                                     %s, %s)
        #                 """, e,)

        sql = """INSERT INTO `access` ( `remote_host`, `remote_logname`, `remote_user`,`date`, `date_utc`,
                                            `request_method`, `request_url`, `request_http_ver`, `status`, `bytes_tx`,
                                            `request_header_referer`, `user_agent`, `user_agent_br_family`,
                                            `user_agent_br_version`, `user_agent_os_family`,
                                            `user_agent_os_version`, `user_agent_is_mobile`)
                                  VALUES (%s, %s, %s, %s, %s,
                                          %s, %s, %s, %s, %s,
                                          %s, %s, %s, %s, %s,
                                          %s, %s)
                        """

        if executeSql == 1:
            cursor.execute(sql, record)

        if counter == 0 and printFirstLine == 1:
            for key in d:
                if isinstance(d[key], datetime.datetime):
                    dtString = d[key]
                    print (key + ": " + dtString.strftime("%Y-%m-%d %H:%M:%S")) # for the keys
                else:
                    print (key + ": " + str(d[key])) # for the keys

        counter += 1

cursor.close()

conn.commit();
conn.close();

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " Finished")
print ("Processed " + str(counter) + " rows")
