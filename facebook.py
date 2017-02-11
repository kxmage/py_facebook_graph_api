#encoding=utf8
import urllib.request
import json
import pymysql.cursors
import datetime

connection = pymysql.connect(host='127.0.0.1',
                             port=3306,
                             user='root',
                             password='123456',
                             db='media_db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
m_sql = 'INSERT INTO facebook_messages (id, message, channel, created_time) VALUES (%s, %s, %s, %s)'
l_sql = 'INSERT INTO facebook_likes (m_id, u_name, u_id) VALUES (%s, %s, %s)'
c_sql = 'INSERT INTO facebook_comments (m_id, c_id, message, u_name, u_id, created_time) VALUES (%s, %s, %s, %s, %s, %s)'

since = "2016-12-05"
until = "2016-12-07%2014:30:00"
channels = ['CCTV.CH','cctvcom','cctvnewschina','ipandacom', 'ILoveAfricaCom', 'cctvenfrancais', 'CCTVAmerica']
token = 'EAACEdEose0cBAEZCjVUWX3239ySxKLQhYmKJwKCxWVllpWGc1s9ZAsC3HTCVzA2RXx0ZBTg50xx25LpvNwf5tZBunnZBltxTpWwYHaTKpj0BhCgBrrnmZCKYEq31hYPxT0v7DpediKVhNJjCKu0TcYSwCQQooLOZAdZCCV5Y8nybXAZDZD'
# f_url = "https://graph.facebook.com/v2.8/CCTV.CH?fields=posts%7Bmessage%7D&access_token=" + token
# res = urllib.request.urlopen(f_url).read().decode('utf-8')
# in_json = json.loads(res)
# # print(in_json)
# print(in_json['posts'])

def f_get(url):
	print(url)
	try:
		ret = urllib.request.urlopen(url).read().decode('utf-8')
		return json.loads(ret)
	except:
		return None
	

def deal_f_likes(likes, m_id):
	if likes == None:
		return
	l_d = likes['data']

	for d in l_d:
		if 'name' in d.keys():
			print(d['name'])
			cursor.execute(l_sql, (m_id, d['name'], d['id']))

	connection.commit()

	if 'paging' in likes.keys():
		if 'next' in likes['paging'].keys():
			deal_f_likes(f_get(likes['paging']['next']), m_id)

def deal_f_comments(comments, m_id):
	if comments == None:
		return
	c_d = comments['data']
	for d in c_d:
		if 'message' in d.keys() and 'id' in d.keys() and 'from' in d.keys() and 'created_time' in d.keys() and 'id' in d['from'].keys() and 'name' in d['from'].keys():
			print(d['from']['name'])
			print(d['message'])
			c_t = datetime.datetime.strptime(d['created_time'], "%Y-%m-%dT%H:%M:%S+0000") + datetime.timedelta(hours=8)
			cursor.execute(c_sql, (m_id, d['id'], d['message'], d['from']['name'], d['from']['id'], c_t))
	
	connection.commit()

	if 'paging' in comments.keys():
		if 'next' in comments['paging'].keys():
			deal_f_comments(f_get(comments['paging']['next']), m_id)

def deal_f_messages(j, channel):
	if j == None:
		return
	d = j['data']
	cnt = len(d)

	for i in d:
		if 'message' in i.keys() and 'id' in i.keys() and 'created_time' in i.keys():
			print (i['message'])
			c_t = datetime.datetime.strptime(i['created_time'], "%Y-%m-%dT%H:%M:%S+0000") + datetime.timedelta(hours=8) 
			cursor.execute(m_sql, (i['id'], i['message'], channel, c_t))
			connection.commit()
		
			mm_url = 'https://graph.facebook.com/v2.8/' + i['id'] + '?fields=likes.limit(500)%2Ccomments.limit(500)&access_token=' + token

			dd = f_get(mm_url)
			if dd == None:
				continue
			if 'likes' in dd.keys():
			 	deal_f_likes(dd['likes'], i['id'])
				
			if 'comments' in dd.keys():
			 	deal_f_comments(dd['comments'], i['id'])

				

	if 'paging' in j.keys():
		if 'next' in j['paging'].keys():
			n_url = j['paging']['next']
			deal_f_messages(f_get(n_url), channel)

	

def facebook_messages(channel):
	m_url = "https://graph.facebook.com/v2.8/" + channel + "?fields=posts.limit(50).since(" + since + ").until(" + until + ")%7Bmessage%2Ccreated_time%7D&access_token=" + token
	#.until(" + until + ")
	in_json = f_get(m_url)
	deal_f_messages(in_json['posts'], channel)

def facebook_d():
	#get messages
	
	for ch in channels:
		facebook_messages(ch)
	




facebook_d()
connection.close();

