# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta
import codecs, threading, pymysql, os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

class WR_SQL:
    def __init__(self, host, user, password, db, style='mysql_pd', port=3306, charset="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.style = style
        self.charset = charset
        self.db = db
        if self.style == 'mysql_execute':
            try:
                # self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.db, charset=self.charset)
                # self.cursor = self.conn.cursor()
                self.database = "mysql+pymysql://%s:%s@%s/%s?charset=utf8" % (self.user, self.password, self.host, self.db)
                self.engine = create_engine(self.database, echo=False, pool_size=100, pool_recycle=3600)
                self.conn = self.engine.connect()
            except pymysql.OperationalError as e:
                print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
        elif self.style == 'mysql_pd':
            self.database = "mysql+pymysql://%s:%s@%s/%s?charset=utf8" %(self.user, self.password, self.host, self.db)
            self.engine = create_engine(self.database, echo=False, pool_size=100, pool_recycle=3600)

    def query(self, sql):
        if self.style == 'mysql_execute':
            if self.conn.closed is True:
                self.engine = create_engine(self.database, echo=False, pool_size=100, pool_recycle=3600)
                self.conn = self.engine.connect()
            self.conn.execute(sql)
        elif self.style == 'mysql_pd':
            self.df = pd.read_sql(sql, self.engine)
            return self.df

    def quit(self):
        if self.style == 'mysql_execute':
            self.conn.close()


host = '123.57.81.203'
db = 'limit_up'
user = 'root'
password = '123456'


QueryDbServer = WR_SQL(host, user, password, db, 'mysql_pd', port=3306)
ExecuDbServer = WR_SQL(host, user, password, db, 'mysql_execute', port=3306)

HOSTNAME = '123.57.81.203'
PORT     = '3306'
DATABASE = 'limit_up'
USERNAME = 'gggin'
PASSWORD = '123456'
DB_URI = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(USERNAME, PASSWORD, HOSTNAME, PORT, DATABASE)
SQLALCHEMY_DATABASE_URI = DB_URI


mysql_engine = create_engine(DB_URI)
DB_Session = sessionmaker(bind=mysql_engine)
session = DB_Session()


