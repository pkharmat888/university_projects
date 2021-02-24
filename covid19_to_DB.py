import urllib3
import json
import mysql.connector
import datetime

# init poolmanager
http = urllib3.PoolManager(num_pools=3)

# getting initial data
response_c = http.request('GET', 'https://api.covid19api.com/dayone/country/czech-republic/status/confirmed')
data_c = json.loads(response_c.data.decode('utf-8'))
response_r = http.request('GET', 'https://api.covid19api.com/dayone/country/czech-republic/status/recovered')
data_r = json.loads(response_r.data.decode('utf-8'))
response_d = http.request('GET', 'https://api.covid19api.com/dayone/country/czech-republic/status/deaths')
data_d = json.loads(response_d.data.decode('utf-8'))

# putting data into database
connector = mysql.connector.connect(user='apti', password='********',
                                    host='127.0.0.1',
                                    database='grafana_data')

# set our table name to store data
tableCases_c = 'confirmed'
tableCases_r = 'recovered'
tableCases_d = 'deaths'
tableCases_c_a = 'confirmed_a'
tableCases_r_a = 'recovered_a'
tableCases_d_a = 'deaths_a'

# drop table if exists, api returning full dataset from dayone
for table in [tableCases_c, tableCases_r, tableCases_d, tableCases_c_a, tableCases_r_a, tableCases_d_a]:
    cursor = connector.cursor()
    try:
        cursor.execute(
            "DROP TABLE " + table)
    except Exception as ex:
        print(ex)


# recreate table
cursor.execute("CREATE TABLE " + tableCases_c + " (time TIMESTAMP PRIMARY KEY, Confirmed INT)")

cursor.execute("CREATE TABLE " + tableCases_r + " (time TIMESTAMP PRIMARY KEY, Recovered INT)")

cursor.execute("CREATE TABLE " + tableCases_d + " (time TIMESTAMP PRIMARY KEY, Deaths INT)")

# insert data
for report in data_c:
    reportTime = datetime.datetime.strptime(report['Date'], '%Y-%m-%dT%H:%M:%SZ')
    reportCases = report['Cases']
    # debugging
    # print('%s %s' % (reportTime, reportCases))
    cursor.execute("INSERT INTO " + tableCases_c + " (time, Confirmed) VALUES (%s, %s)", (reportTime, reportCases))

for report in data_r:
    reportTime = datetime.datetime.strptime(report['Date'], '%Y-%m-%dT%H:%M:%SZ')
    reportCases = report['Cases']
    # debugging
    # print('%s %s' % (reportTime, reportCases))
    cursor.execute("INSERT INTO " + tableCases_r + " (time, Recovered) VALUES (%s, %s)", (reportTime, reportCases))

for report in data_d:
    reportTime = datetime.datetime.strptime(report['Date'], '%Y-%m-%dT%H:%M:%SZ')
    reportCases = report['Cases']
    # debugging
    # print('%s %s' % (reportTime, reportCases))
    cursor.execute("INSERT INTO " + tableCases_d + " (time, Deaths) VALUES (%s, %s)", (reportTime, reportCases))

# create moving_average_tables
cursor.execute(
    "CREATE TABLE " + tableCases_c_a + " AS (SELECT *, AVG(Confirmed) OVER(ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_c FROM confirmed)")
cursor.execute(
    "CREATE TABLE " + tableCases_r_a + " AS (SELECT *, AVG(Recovered) OVER(ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_r FROM recovered)")
cursor.execute(
    "CREATE TABLE " + tableCases_d_a + " AS (SELECT *, AVG(Deaths) OVER(ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_d FROM deaths)")

# Make sure data is committed to the database
connector.commit()

# free handlers
cursor.close()
connector.close()
