import pymongo
import sqlalchemy
from time import gmtime, strftime, sleep
import hashlib
import time as tm
import hmac
import hashlib
import base64
import urllib.parse
import requests
import hashlib
import pymysql as mysql
from sshtunnel import SSHTunnelForwarder
import os
from dotenv import load_dotenv
load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv('credential')
project_id = os.getenv('project_id')
ssh_key =  os.getenv('ssh_pkey')


# --------------- DB connect ---------------
def hermes_m2():
    engine = sqlalchemy.create_engine('postgresql://pbi:4xTSCT3T5h2Y4jhU@gobiz-db-instance.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com/hermes')
    conn = engine.connect()
    return conn

def fobiz():
    engine = sqlalchemy.create_engine('postgresql://pbi:honnguyenlaoquai@fobiz.cluster-ro-c6fkp4r9znqb.ap-southeast-1.rds.amazonaws.com/fobiz')
    conn = engine.connect()
    return conn
def m2():
    conn = sqlalchemy.create_engine('postgresql://pbi:4xTSCT3T5h2Y4jhU@gobiz-db-instance.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com/poseidon')
    conn = conn.connect()
    return conn
def m3():
    conn = sqlalchemy.create_engine('postgresql://pbi:4xTSCT3T5h2Y4jhU@gobiz-db-instance.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com/themis')
    conn = conn.connect()
    return conn
def m19():
    conn = sqlalchemy.create_engine('postgresql://nguyenvanquy:Tr@mAnhTh3Ph13t@gobiz-production.cluster-ro-cwrilkx3imys.ap-southeast-1.rds.amazonaws.com/m19')
    conn = conn.connect()
    return conn


def m6():
    server = SSHTunnelForwarder(
        ssh_address_or_host=("bastion.vinasat.gobiz.dev", 22),
        ssh_username="m30",
        ssh_pkey= ssh_key,
        remote_bind_address=('m6-mysql.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com', 3306))
    server.start()
    connection = mysql.connect(user="m30", passwd="7y1zGzbB9iIsVePi", host='localhost', port=server.local_bind_port, database="m6")
    return connection

def m28():
    server = SSHTunnelForwarder(
        ssh_address_or_host=("bastion.vinasat.gobiz.dev", 22),
        ssh_username="m30",
        ssh_pkey= ssh_key ,
        remote_bind_address=('m6-mysql.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com', 3306))
    server.start()
    connection = mysql.connect(user='m30', passwd='7y1zGzbB9iIsVePi', host='localhost', port=server.local_bind_port, database='m28')
    return connection


def m5():
    server = SSHTunnelForwarder(
        ssh_address_or_host=("bastion.vinasat.gobiz.dev", 22),
        ssh_username="m30",
        ssh_pkey= ssh_key,
        remote_bind_address=('m5-mariadb.cvzp5vitrkjj.ap-southeast-1.rds.amazonaws.com', 3306))
    server.start()
    connection = mysql.connect(user="m30", passwd="7y1zGzbB9iIsVePi", host='localhost', port=server.local_bind_port, database="m5")
    return connection

def bg():
    server = SSHTunnelForwarder(
        ssh_address_or_host=('47.91.217.148', 22),
        ssh_username='order_baogam',
        ssh_pkey= ssh_key,
        remote_bind_address=('127.0.0.1', 3306))
    server.start()
    connection = mysql.connect(user='viewer', passwd='6iEvw3hKPEiY247H', host='127.0.0.1', port=server.local_bind_port)
    return connection

def nhaphang():
    server = SSHTunnelForwarder(
        ssh_address_or_host=('47.52.89.113', 22),
        ssh_username='seudo',
        ssh_pkey= ssh_key,
        remote_bind_address=('127.0.0.1', 3306))
    server.start()
    connection = mysql.connect(user='viewer', passwd='h592tITLFVcuR3gI', host='127.0.0.1', port=server.local_bind_port)
    return connection

def data_call_NH():
    MONGO_HOST = '47.52.112.39'
    SERVER_USER = 'logistic'
    PRIVATE_KEY =  ssh_key
    MONGO_USER = 'logistic'
    MONGO_PASS = 'mdPM1pKnKHPC'
    MONGO_DB = "logistic"
    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=SERVER_USER,
        ssh_pkey=PRIVATE_KEY,
        remote_bind_address=('127.0.0.1', 27017))
    server.start()
    connection = pymongo.MongoClient('localhost', server.local_bind_port)
    db = connection[MONGO_DB]
    db.authenticate(MONGO_USER, MONGO_PASS)
    return db

def data_call_baogam():
    MONGO_HOST = '47.91.214.160'
    SERVER_USER = 'logistic'
    PRIVATE_KEY = ssh_key
    MONGO_USER = 'logistic_baogam'
    MONGO_PASS = 'LHFVOzuuw_s8DoF8'
    MONGO_DB = "logistic_baogam"
    # define ssh tunnel
    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=SERVER_USER,
        ssh_pkey=PRIVATE_KEY,
        remote_bind_address=('127.0.0.1', 27017))
    server.start()
    connection = pymongo.MongoClient('localhost', server.local_bind_port)
    db = connection[MONGO_DB]
    db.authenticate(MONGO_USER, MONGO_PASS)
    return db

def hash_string(string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()

def sent_noti_success(schema):
        access_token = "3f25e4480edf11236769bdfde4775876d216445d9c07dfe5f3f10d9f89f130f9"
        timestamp = str(round(tm.time() * 1000))
        secret = "SEC5f104c0d819540ef01f730fd9b885c3dad4b1b87df7f4803e00405e1d4822174"
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        data = {"msgtype": "markdown",
                "markdown": {
                    "title": "Notice from chatbot",
                    "text": "đã chạy " + schema
                        }}
        msg = requests.request("POST","https://oapi.dingtalk.com/robot/send?access_token={}&sign={}&timestamp={}".format(access_token, sign, timestamp), json=data)

def sent_noti(schema,table,error):
        access_token = "3f25e4480edf11236769bdfde4775876d216445d9c07dfe5f3f10d9f89f130f9"
        timestamp = str(round(tm.time() * 1000))
        secret = "SEC5f104c0d819540ef01f730fd9b885c3dad4b1b87df7f4803e00405e1d4822174"
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        data = {"msgtype": "markdown",
                "markdown": {
                    "title": "Notice from chatbot",
                    "text": "Lỗi đồng bộ bảng: " + table + " thuộc Schema: " + schema + ": "+str(error)
                        }}
        msg = requests.request("POST","https://oapi.dingtalk.com/robot/send?access_token={}&sign={}&timestamp={}".format(access_token, sign, timestamp), json=data)


def sent_noti_m28_fobiz(schema,table,error):
    access_token = "e385a66180eb00e00587060a4ea03f82f4cbae07efc068a4a4a0cd7c50691105"
    timestamp = str(round(tm.time() * 1000))
    secret = "SECdbdcfb13e5f0d549637f3e4fd263e69ff0ec01bca23760553cbc9cd1d9135244"
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    data = {"msgtype": "markdown",
            "markdown": {
                "title": "Notice from chatbot",
                "text": "Lỗi đồng bộ bảng: " + table + " thuộc Schema: " + schema + ": "+str(error)
                    }}
    msg = requests.request("POST","https://oapi.dingtalk.com/robot/send?access_token={}&sign={}&timestamp={}".format(access_token, sign, timestamp), json=data)



def convert_type_bigquery(c):
    for l in c:
        for key, value in l.items():
            if value == "jsonb":
                l[key] = "string"
            elif value == "json":
                l[key] = "string"
            elif value == "real":
                l[key] = "float64"
            elif value == "character varying":
                l[key] = "string"
            elif value == "char":
                l[key] = "string"
            elif value == "varchar":
                l[key] = "string"
            elif value == "text":
                l[key] = "string"
            elif value == "enum":
                l[key] = "string"
            elif value == "tinytext":
                l[key] = "string"
            elif value == "mediumtext":
                l[key] = "string"
            elif value == "longtext":
                l[key] = "string"
            elif value == "timestamp with time zone":
                l[key] = "TIMESTAMP"
            elif value == "double precision":
                l[key] = "float"
            elif value == "double":
                l[key] = "float"
            elif value == "decimal":
                l[key] = "float"
            elif value == "timestamp without time zone":
                l[key] = "TIMESTAMP"
            elif value == "time with time zone":
                l[key] = "string"
            elif value == "bigint":
                l[key] = "int64"
            elif value == "smallint":
                l[key] = "int64"
            elif value == "numeric":
                l[key] = "float64"
            elif value == "tsvector":
                l[key] = "string"
            elif value == "USER-DEFINED":
                l[key] = "string"
            elif value == "list":
                l[key] = "string"
            elif value == 'uuid':
                l[key] = "string"
            elif value == "int":
                l[key] = "integer"
            elif value == "tinyint":
                l[key] = "integer"
            elif value == "datetime":
                l[key] = 'timestamp'
    return c

# query_to_local = """INSERT INTO log_job.realtime_airflow
#                     (dataset, table_name, execution_date, start_date,end_date, state, error, row_number)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                     ON DUPLICATE KEY UPDATE
#                     start_date = %s,
#                     end_date = %s,
#                     state = %s,
#                     error = %s,
#                     row_number = %s
#                 """

def flatten_json(nested_json):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def get_schema(client, project_id, your_dataset, your_table_name):
    table_id = project_id + '.' + your_dataset + '.' + your_table_name
    table = client.get_table(table_id)  # Make an API request.
    result = [{'name': schema.name, 'type': schema.field_type} for schema in table.schema]
    return result