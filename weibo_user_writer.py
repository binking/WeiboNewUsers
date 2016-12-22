#coding=utf-8
import traceback
from datetime import datetime as dt
from template.weibo_writer import DBAccesor, database_error_hunter


class WeiboUserWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_new_user_into_db(self, info_dict):
        uri = info_dict['uri']; clueid = ''; fullpath = uri
        realpath = uri; middle = 'default'; bucketName = 'follower'
        theme = '新浪微博_博主详细信息48992_daily'
        createdate = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_new_user_sql = """
            INSERT INTO WeiboUser (clueid, fullpath, realpath, 
            theme,  middle, createdate, bucketName,
            uri, weibo_user_url, nickname, gender, introduction, realname, location, 
            registration_date, label, date_of_birth, company,preliminary_school,
            middle_school, high_school, tech_school, university, blog_url, domain, 
            msn, QQ, email, sex_tendancy, emotion, blood_type, focus_num, fans_num, 
            weibo_num , KOL)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS(SELECT * FROM WeiboUser WHERE weibo_user_url = %s)
        """
        insert_label_sql = """
            INSERT INTO WeiboUserLabel(weibo_user_url, label)  
            SELECT %s, %s FROM DUAL WHERE NOT EXISTS(
            SELECT * FROM WeiboUserLabel WHERE weibo_user_url=%s AND label=%s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(insert_new_user_sql,(
                clueid, fullpath, realpath, theme, middle,
                createdate, bucketName, uri, info_dict['weibo_user_url'],
                info_dict.get('nickname', ''), info_dict.get('gender', ''), 
                info_dict.get('introduction', ''), info_dict.get('realname', ''),
                info_dict.get('location', ''), info_dict.get('registration_date', ''),
                info_dict.get('label', ''), info_dict.get('date_of_birth', ''),
                info_dict.get('company', ''), info_dict.get('preliminary_school', ''),
                info_dict.get('middle_school', ''), info_dict.get('high_school', ''),
                info_dict.get('high_school', ''), info_dict.get('university', ''),
                info_dict.get('blog_url', ''), info_dict.get('domain', ''),
                info_dict.get('msn', ''), info_dict.get('qq', ''),
                info_dict.get('email', ''), info_dict.get('sex_tendancy', ''),
                info_dict.get('emotion', ''), info_dict.get('blood_type', ''),
                info_dict.get('focus_num', -1),info_dict.get('fans_num', -1), 
                info_dict.get('weibo_num', -1), info_dict.get('kol', ''),
                uri)):
            import ipdb; ipdb.set_trace()
            print '$'*10, "1. Insert %s SUCCEED." % uri
        if info_dict.get('label'):
            labels = info_dict['label'].split(' ')
            if cursor.executemany(insert_label_sql, 
                [(uri, label, uri, label) for label in labels]):
                import ipdb; ipdb.set_trace()
                print '$'*10, "2. Write label SUCCEED."
        # conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_new_user_from_db(self):
        select_new_user_sql = """
            SELECT DISTINCT concat(CommentAuthor.weibocomment_author_url, '/info') FROM 
            (SELECT  wc.weibocomment_author_url FROM topicinfo t, topicweiborelation twr, weibocomment wc
            WHERE t.createdate > date_sub(now(), INTERVAL '7' DAY)
            AND t.topic_url = twr.topic_url
            AND twr.weibo_url = wc.weibo_url
            ) AS CommentAuthor LEFT JOIN WeiboUser wu ON CommentAuthor.weibocomment_author_url = wu.weibo_user_url 
            WHERE wu.weibo_user_url IS NULL LIMIT 10
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_new_user_sql)
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