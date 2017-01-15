#coding=utf-8
import traceback
from datetime import datetime as dt
from zc_spider.weibo_writer import DBAccesor, database_error_hunter


class WeiboUserWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    def insert_new_user_into_db(self, info):
        uri = info['uri']; fullpath = uri
        realpath = uri; middle = 'default'; bucketName = 'follower'
        theme = '新浪微博_博主详细信息python_daily'
        createdate = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_new_user_sql = """
            INSERT INTO WeiboUser (
            fullpath, realpath, theme,  middle, createdate, 
            bucketName, uri, weibo_user_url, weibo_user_card, nickname, 
            gender, introduction, realname, location, registration_date, 
            label, date_of_birth, company, preliminary_school, middle_school, 
            high_school, tech_school, university, blog_url, domain, 
            msn, QQ, email, sex_tendancy, emotion, 
            blood_type, focus_num, fans_num, weibo_num, KOL)
            SELECT 
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s 
            FROM DUAL WHERE NOT EXISTS(
            SELECT * FROM WeiboUser WHERE weibo_user_url = %s)
        """
        insert_label_sql = """
            INSERT INTO WeiboUserLabel(weibo_user_url, label)  
            SELECT %s, %s FROM DUAL WHERE NOT EXISTS(
            SELECT * FROM WeiboUserLabel WHERE weibo_user_url=%s AND label=%s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(insert_new_user_sql,(
                fullpath, realpath, theme, middle,createdate, 
                bucketName, uri, info['weibo_user_url'], info['uid'],
                info.get('nickname', ''), info.get('gender', ''), 
                info.get('introduction', ''), info.get('realname', ''),
                info.get('location', ''), info.get('registration_date', ''),
                info.get('label', ''), info.get('date_of_birth', ''),
                info.get('company', ''), info.get('preliminary_school', ''),
                info.get('middle_school', ''), info.get('high_school', ''),
                info.get('high_school', ''), info.get('university', ''),
                info.get('blog_url', ''), info.get('domain', ''),
                info.get('msn', ''), info.get('qq', ''),
                info.get('email', ''), info.get('sex_tendancy', ''),
                info.get('emotion', ''), info.get('blood_type', ''),
                info.get('focus_num', 0),info.get('fans_num', 0), 
                info.get('weibo_num', 0), info.get('kol', ''),
                uri)):
            print '$'*10, "1. Insert %s SUCCEED." % uri
        if info.get('label'):
            labels = info['label'].split(' ')
            if cursor.executemany(insert_label_sql, 
                [(uri, label, uri, label) for label in labels]):
                print '$'*10, "2. Write label SUCCEED."
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_new_user_from_db(self):
        select_new_user_sql = """
            SELECT DISTINCT concat(CommentAuthor.weibocomment_author_url, '/info') FROM (
            SELECT wc.weibocomment_author_url 
            FROM topicinfo t, topicweiborelation twr, weibocomment wc
            WHERE t.createdate > date_sub(now(), INTERVAL '5' DAY)
            AND t.topic_url = twr.topic_url
            AND twr.weibo_url = wc.weibo_url) AS CommentAuthor 
            WHERE NOT EXISTS (
            SELECT 1 FROM WeiboUser wu 
            WHERE CommentAuthor.weibocomment_author_url = wu.weibo_user_url) 
            -- Update one day user per day
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_new_user_sql)
        for res in cursor.fetchall():
            yield res[0]

    @database_error_hunter
    def read_repost_user_from_db(self):
        select_sql = """
            SELECT DISTINCT CONCAT('http://weibo.com/', wr.weibo_user_id , '/info')
            FROM weiboreposts wr
            -- where not EXISTS (
            -- select * from weibouser wu
            -- where wu.weibo_user_card=wr.weibo_user_id
            -- );
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for res in cursor.fetchall():
            yield res[0]
"""
What is cursor ???
_result None
description None
rownumber 0
messages []
_executed 
            INSERT INTO WeiboUser (clueid, fullpath, realpath, 
            theme,  middle, createdate, bucketName,
            uri, weibo_user_url, nickname, gender, introduction, realname, location, 
            registration_date, label, date_of_birth, company,preliminary_school,
            middle_school, high_school, tech_school, university, blog_url, domain, 
            msn, QQ, email, sex_tendancy, emotion, blood_type, focus_num, fans_num, 
            weibo_num , KOL)
            SELECT '','http://weibo.com/1681897083/info','http://weibo.com/1681897083/info','新浪微博_博主详细信息48992_daily','default','2016-12-22 14:00:57','follower','http://weibo.com/1681897083/info','http://weibo.com/1681897083','连慧勇N',
            '男','海纳百川','','其他','2011-11-11','','','','','','','',
            '河南工程学院','','','','','','','','',343,187,211,''
            FROM DUAL WHERE NOT EXISTS(SELECT * FROM WeiboUser WHERE weibo_user_url = 'http://weibo.com/1681897083/info')
        
errorhandler <bound method Connection.defaulterrorhandler of <_mysql.connection open to '10.66.110.147' at 1358eb0>>
rowcount 1
connection <_mysql.connection open to '10.66.110.147' at 1358eb0>
description_flags None
arraysize 1
_info Records: 1  Duplicates: 0  Warnings: 0
lastrowid 4604478
_last_executed 
            INSERT INTO WeiboUser (clueid, fullpath, realpath, 
            theme,  middle, createdate, bucketName,
            uri, weibo_user_url, nickname, gender, introduction, realname, location, 
            registration_date, label, date_of_birth, company,preliminary_school,
            middle_school, high_school, tech_school, university, blog_url, domain, 
            msn, QQ, email, sex_tendancy, emotion, blood_type, focus_num, fans_num, 
            weibo_num , KOL)
            SELECT '','http://weibo.com/1681897083/info','http://weibo.com/1681897083/info','新浪微博_博主详细信息48992_daily','default','2016-12-22 14:00:57','follower','http://weibo.com/1681897083/info','http://weibo.com/1681897083','连慧勇N',
            '男','海纳百川','','其他','2011-11-11','','','','','','','',
            '河南工程学院','','','','','','','','',343,187,211,''
            FROM DUAL WHERE NOT EXISTS(SELECT * FROM WeiboUser WHERE weibo_user_url = 'http://weibo.com/1681897083/info')
        
_warnings 0
_rows ()
"""

# insert_new_user_sql = """
#     INSERT INTO weibouser
#     (theme, middle, createdate,
#     bucketName, uri  , weibo_user_url, nickname, realname, 
#     gender, introduction, location, registration_date,
#     label, date_of_birth , company, preliminary_school,
#     middle_school, high_school, tech_school, university, blog_url, 
#     domain, msn, QQ, email, sex_tendancy, emotion, blood_type, 
#     KOL, focus_num, fans_num, weibo_num)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
#     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
#     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
# """
# is_exited_sql = """
#     SELECT id FROM WeiboUser
#     WHERE uri=%s
# """
 # cursor.execute(is_exited_sql, (uri, ))
# if cursor.rowcount:
#     print 'The user %s existed...' % uri