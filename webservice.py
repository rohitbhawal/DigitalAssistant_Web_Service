'''
Created on Oct 1, 2016
@author: Rohit Bhawal
'''

import json
import os
import pymysql
from flask import Flask, render_template, request
from datetime import timedelta


app = Flask(__name__)
Uname = ''

@app.route('/executeQuery')
def query():
    connection = ""
    result = ""
    try:
        if request.method == "GET":
            if 'query' in request.args:
                query = request.args['query'] 
                print query
            else:
                return "Error: Invalid Query"
            connection = ConnectDB() 
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                print result
                if result:
#                     print result
#                     print result[0]['dateVal']
#                     print str(result[0]['dateVal'])
                    for i in range(len(result)):
                        if 'dateVal' in result[i]:
                            result[i]['dateVal'] = str(result[i]['dateVal'])
#                     print result
                    result = json.dumps(result)
#                     print result
                else:
                    result = "Error: No Records Found"
        return result
    except Exception as e:
        return "Error: "+ str(e)
    finally:
        if connection:
            connection.close()

@app.route('/commitQuery')
def commit():
    connection = ""
    result = ""
    try:
        if request.method == "GET":
            if 'query' in request.args:
                query = request.args['query'] 
                print query
            else:
                return "Error: Invalid Query"
            connection = ConnectDB() 
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                connection.commit()
                result = "Success"
        return result
    except Exception as e:
        return "Error: "+ str(e)
    finally:
        if connection:
            connection.close()

  
@app.route('/insertUser')
def insertUser():
    connection = ""
    result = ""
    try:
        if request.method == "GET":
            if 'userid' in request.args:
                userid = request.args['userid'] 
            else:
                return "Error: UserId Missing"
             
            if 'name' in request.args:
                name = request.args['name'] 
            else:
                return "Error: Name Missing"
             
            if 'pasw' in request.args:
                pasw = request.args['pasw'] 
            else:
                return "Error: Password Missing"
             
            if 'mobile' in request.args:
                mobile = request.args['mobile'] 
            else:
                return "Error: Mobile Missing"
             
            if 'email' in request.args:
                email = request.args['email'] 
            else:
                return "Error: Email Missing"
            query = "insert into `User` (`userid`, `name`, `pasw`, `mobile`, `email`) \
                     values ('%s', '%s', '%s', '%s', '%s' ) " % (userid, name, pasw, mobile, email)
            connection = ConnectDB() 
            print query
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
                result = cursor.fetchall()
                print result
                if result:
                    result = "Error: Insert User Failure"
                else:
                    result = "Success"
        return result
    except Exception as e:
        return "Error: "+ str(e)
    finally:
        if connection:
            connection.close()
 
@app.route('/insertTask')
def insertTask():
    connection = ""
    result = ""
    try:
        if request.method == "GET":
            if 'userid' in request.args:
                userid = request.args['userid'] 
            else:
                return "Error: UserId Missing"
             
            if 'taskid' in request.args:
                taskid = request.args['taskid'] 
            else:
                return "Error: Taskid Missing"
             
            if 'longitude' in request.args:
                longitude = request.args['longitude'] 
            else:
                return "Error: Longitude Missing"
             
            if 'latitude' in request.args:
                latitude = request.args['latitude'] 
            else:
                return "Error: latitude Missing"
             
            if 'addr' in request.args:
                addr = request.args['addr'] 
            else:
                return "Error: addr Missing"
            
            if 'desc' in request.args:
                desc = request.args['desc'] 
            else:
                return "Error: Description Missing"
            
            if 'started' in request.args:
                started = request.args['started'] 
            else:
                return "Error: Description Missing"
            
            if 'range' in request.args:
                range = request.args['range'] 
            else:
                return "Error: range Missing"
            
            if 'date' in request.args:
                date = request.args['date']
                print date
                if date:
                    date = formatDate(date)
                print date
            else:
                return "Error: date Missing"
            
            if 'rep' in request.args:
                rep = request.args['rep']
                print rep
#                 vailid_Days = getValidDays(rep)
#                 print vailid_Days
            else:
                rep = ''
            
            query = "insert into `Task` (`userid`, `taskid`, `longitude`, `latitude`, `addr`, `desc`, \
                                        `started`, `rangeVal`, `dateVal`) \
                     values ('%s', '%s', %s, %s, '%s' , '%s', '%s', %s, '%s', '%s') "  \
                     % (userid, taskid, longitude, latitude, addr, desc, started, range, date)
            connection = ConnectDB() 
            print query
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
                result = cursor.fetchall()
                print result
                if result:
                    result = "Error: Insert Task Failure"
                else:
                    result = "Success"
        return result
    except Exception as e:
        return "Error: "+ str(e)
    finally:
        if connection:
            connection.close()

@app.route('/validTask')
def checkTask():
    taskid = ''
    connection = ""
    result = ""
    unit = 'km'
    if unit in 'km':
        factLat = 110.574
        factLon = 111.320
        MulFact = 1.60934
    else:
        factLat = 68.707
        factLon = 69.171
        MulFact = 1
    
    try:
        if request.method == "GET":
            if 'userid' in request.args:
                userid = request.args['userid'] 
                print userid
            else:
                return "Error: userid Query"
                        
#             if 'range' in request.args:
#                 range = request.args['range'] 
#                 print range
#             else:
#                 return "Error: range Query" 
            
            if 'latitude' in request.args:
                latitude = request.args['latitude'] 
                latitude = float(latitude)
                print latitude
            else:
                return "Error: latitude Query" 
            
            if 'longitude' in request.args:
                longitude = request.args['longitude'] 
                longitude = float(longitude)
                print longitude
            else:
                return "Error: longitude Query"
            
            if 'date' in request.args:
                date = request.args['date'] 
                print date
            else:
                return "Error: date Query"
            
            if 'taskid' in request.args:
                taskid = request.args['taskid']
            else:
                taskid = ''
            
            if 'rep' in request.args:
                rep = request.args['rep']
                print rep
                vailid_Day = getValidDays(rep,date)
                print vailid_Day
            else:
                rep = ''
            
            connection = ConnectDB() 
            with connection.cursor() as cursor:
                query = buildQuery(userid, date, latitude, longitude, factLat, factLon, MulFact, taskid)
                print query
                cursor.execute(query)
                result = cursor.fetchall()
                print result
                if result:
                    for i in range(len(result)):
                        if 'dateVal' in result[i]:
                            result[i]['dateVal'] = str(result[i]['dateVal'])
#                             print result
                    result = json.dumps(result)
                    print result
                else:
                    result = "Error: No Records Found"
        return result
    except Exception as e:
        return "Error: "+ str(e)
    finally:
        if connection:
            connection.close()

def buildQuery(userid, date, latitude, longitude, factLat, factLon, MulFact, taskid):
    distance = " 3956 * 2 * ASIN(SQRT( POWER(SIN(( %f - latitude)*pi()/180/2),2) \
                + COS( %f *pi()/180 )*COS(latitude*pi()/180)*POWER(SIN(( %f -longitude)*pi()/180/2),2))) * %f" \
                % (latitude, latitude, longitude, MulFact)
    select = " select *, "+ distance + " as Distance from Task " 
    where =  " where userid = '%s' and started = 'yes' and dateVal = '%s' " % (userid, date)
    where1 = " and latitude <= %f + rangeVal/%f " % (latitude, factLat)
    where2 = " and latitude >= %f - rangeVal/%f " % (latitude, factLat)
    where3 = " and longitude <= %f + rangeVal/(%f*cos(radians(%f))) " % (longitude, factLon, latitude)
    where4 = " and longitude >= %f - rangeVal/(%f*cos(radians(%f)))" % (longitude, factLon, latitude) 
    where5 = " and " + distance + " <= rangeVal " 
    if taskid:
        where5 = " and taskid = '%s' " % (taskid)
        where1 = "";
        where2 = "";
        where3 = "";
        where4 = "";
    orderBy = " order by taskid"
    
    query = select + where + where1 + where2 + where3 + where4 + where5 + orderBy
    return query

# Days 1111111 -> Mon Tue Wed Thu Fri Sat Sun
def getValidDays(rep, todayDate):
    days = []
    for i in range(len(rep)):
        if rep[i] == '1':
            days.append(i)
    if not days:
        return False
    
    
    
    return days      

def ConnectDB():
    connection = pymysql.connect(host='HOST NAME',
                                 user='USERID',
                                 password='PASSWORD',
                                 db='YOUR DB',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def formatDate(date):
    return date.split("/")[2] + "/" + date.split("/")[0] + "/" + date.split("/")[1]


port = os.getenv('VCAP_APP_PORT','443')
if __name__ == '__main__':
    app.secret_key = os.urandom(24)
    app.permanent_session_lifetime = timedelta(seconds=60)
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True)
    