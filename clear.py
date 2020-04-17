# -*- coding: utf-8 -*-

import PySimpleGUI as sg
import time
import asyncio
import sqlite3
import threading
import queue
import logging
import os
import json
import aiohttp
import random
import sys
import xlsxwriter   #导入模块
g_all_num = 0
g_related_uid_list = []
delay = 0.8
g_total_num = 0
g_stop = False
threads = []
g_ranking_tags = []

def save_user(cursor,conn,uid,fav,owned,refav,reowned,t1,t2):
    sql = f'insert into  user( id,owned_num,fav_num,fav_related_num,owned_related_num,\
        fav_related_bvids,owned_related_bvids,owned_bvids,fav_bvids,last_owned_time,last_fav_time) \
        values ({uid},{len(owned)},{len(fav)},{len(refav)},{len(reowned)},\
            \"{refav}\",\"{reowned}\",\"{owned}\",\"{fav}\",{t1},{t2})'
    try:
        cursor.execute(sql)
        conn.commit()
    except BaseException as e:
        print(sql)
        logging.error(e) # 错误
#单收藏夹视频
async def fetch_sigle_list_favs_bvids(session,mid,pn=1):    
    if g_stop:
        return 0,[]        
    await asyncio.sleep(delay)
    fav_uri = f"https://api.bilibili.com/x/v3/fav/resource/list?media_id={mid}&pn={pn}&ps=20&keyword=&order=mtime&type=0&tid=0&jsonp=jsonp"
    bvids = []
    async with session.get(fav_uri) as response:
        try:
            data =  json.loads(await response.text())
            
            if(data['code'] == 0 and  data['data']['medias']):
                videos = data['data']['medias']
                for v in videos:
                    bvids.append(v['bvid'])
                if pn*20>=data['data']['info']['media_count']  or pn > 20:
                    if pn == 1 :
                        return videos[0]['fav_time'],bvids
                    return bvids
                res = await fetch_sigle_list_favs_bvids(session,mid,pn+1)

                if pn == 1 :
                    return videos[0]['fav_time'],bvids + res
                return bvids + res
        except BaseException as e:
            logging.error("uri:",fav_uri,e) 
            if pn == 1 :
                return 0,[]
            return []
    if pn == 1 :
        return 0,[]
    return []
#收藏视频
async def fetch_favs_bvids(session,uid,maxt):
    print('收藏视频')
    if g_stop:
        return 0,[]
    await asyncio.sleep(delay)
    uri = f"https://api.bilibili.com/x/v3/fav/folder/created/list?pn=1&ps=100&up_mid={uid}&jsonp=jsonp"
    fav_list=[]
    async with session.get(uri) as response:
        try:
            data =  json.loads( await response.text())
            if(data['code'] == 0 and data['data']):
                for d in data['data']['list']:
                    fav_list.append(d['id'])
            else:
                return 0,[]

            tasks_container = [fetch_sigle_list_favs_bvids(session,f) for f in fav_list]
            
            #降速,防止有人变态的搞了几十个收藏夹
            tasks = []
            bvids = []
            fav_times = []
            for task in tasks_container:
                tasks.append(task)
                if(len(tasks)>=maxt):
                    results = await asyncio.gather(*tasks)
                    for t,bv in results:
                        fav_times.append(t)
                        bvids+=bv
                    tasks=[]
            
            results = await asyncio.gather(*tasks)
            for t,bv in results:
                fav_times.append(t)
                bvids+=bv
            return max(fav_times),bvids
        except BaseException as e:
            logging.error("uri:",uri,e) 
            return 0,[]
#视频标签
async def fetch_video_by_bvid(session,bvid):
    if g_stop:
        return bvid,[]
    print('视频标签')
    uri = f"https://api.bilibili.com/x/tag/archive/tags?bvid={bvid}&jsonp=jsonp"
    tags=[]
    await asyncio.sleep(delay)
    async with session.get(uri) as response:
        try:
            data =  json.loads(await response.text())
            if(data['code'] == 0):
                for d in data['data']:
                    if '\'' not in d['tag_name'] and '\"' not in d['tag_name']:
                        tags.append(d['tag_name'])
            else:
                logging.error("uri:",uri,e) 
        except BaseException as e:
            logging.error("uri:",uri,e) 
    return bvid,tags
#拥有视频 
async def fetch_owned_bvids(session,uid,pn=1):
    print('拥有视频')
    await asyncio.sleep(delay)
    uri  = f"https://api.bilibili.com/x/space/arc/search?pn={pn}&ps=100&order=pubdate&keyword=&mid={uid}"
    bvids = []
    if g_stop:
        return 0,[]
    async with session.get(uri) as response:
        try:
            data =  json.loads(await response.text())
            if(data['code'] == 0 and data['data']['list']['vlist']):
                videos = data['data']['list']['vlist']
                for v in videos:
                    bvids.append(v['bvid'])
                if(pn*100>=data['data']['page']['count'] or pn > 10):
                    if pn == 1 :
                        return videos[0]['created'],bvids
                    return bvids
                res =  await fetch_owned_bvids(session,uid,pn+1)
                
                if pn == 1 :
                    return videos[0]['created'],bvids + res
                return bvids + res
        except BaseException as e:
            logging.error("uri:",uri,e) 
            if pn == 1 :
                return 0,[]
            return []
    if pn == 1 :
        return 0,[]
    return []
#所有视频
async def fetch_user_favs_and_bvids(uid,maxt,tags_need):  
    global g_related_uid_list  
    global g_all_num
    global g_ranking_tags
    print('所有视频')
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:

            #获取最新发布时间和所有发布bv
            res2 = await asyncio.gather(fetch_owned_bvids(session,uid))
            last_owned_time,owned_bvids = res2[0]

            # 获取最新收藏时间和所有收藏bv
            res1 = await asyncio.gather(fetch_favs_bvids(session,uid,maxt))
            last_fav_time,fav_bvids = res1[0]

            if last_owned_time == 0 and last_fav_time == 0:
                return
            g_all_num +=1
            #获取owned标签
            tasks = []
            owned_related = []
            owned = [] 

            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                    for bvid in owned_bvids:
                        tasks.append(fetch_video_by_bvid(session,bvid))
                        tagsList = []
                        if(len(tasks)>=maxt):
                            tagsList = await asyncio.gather(*tasks)
                            tasks = []
                            for bvid,tags in tagsList:
                                owned.append((bvid,tags))
                                for tn in tags_need:
                                    if tn in tags:
                                        owned_related.append((bvid,tags))
                    tagsList = await asyncio.gather(*tasks)
                    tasks = []  
                    for bvid,tags in tagsList:
                        owned.append((bvid,tags))
                        for tn in tags_need:
                            if tn in tags:
                                owned_related.append((bvid,tags))
            except BaseException as e:
                    logging.error("获取owned标签错误",e) 


            # # 获取fav标签
            tasks = []
            fav_related = []
            fav = [] 
            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                    for bvid in fav_bvids:
                        tasks.append(fetch_video_by_bvid(session,bvid))
                        tagsList = []
                        if(len(tasks)>=maxt):
                            tagsList = await asyncio.gather(*tasks)
                            tasks = []
                            for bvid,tags in tagsList:
                                fav.append((bvid,tags))
                                for tn in tags_need:
                                    if tn in tags:
                                        fav_related.append((bvid,tags))
                    tagsList = await asyncio.gather(*tasks)
                    tasks = []  
                    for bvid,tags in tagsList:
                        fav.append((bvid,tags))
                        for tn in tags_need:
                            if tn in tags:
                                fav_related.append((bvid,tags))
            except BaseException as e:
                    logging.error("获取fav标签错误",e) 

            if len(fav_related)>0 or len(owned_related)>0:
                g_related_uid_list.append(uid)
                conn = sqlite3.connect(os.path.dirname(__file__)+'/cache.db')
                cursor = conn.cursor()
                save_user(cursor,conn,uid,[x for x,y in fav],[x for x,y in owned],[x for x,y in fav_related],[x for x,y in owned_related],last_owned_time,last_fav_time)

                print(len(fav),len(owned))
                for x,y in fav:
                    g_ranking_tags+=y
                    cursor.execute(f'insert into video(bvid,tags) values (\"{x}\",\"{y}\")')
                for x,y in owned:
                    g_ranking_tags+=y 
                    cursor.execute(f'insert into video(bvid,tags) values (\"{x}\",\"{y}\")')
                conn.commit()


            
    except BaseException as e:
        print(e)
#用户分离函数
async def user_go(utype,maxu,maxt,tags_need):
    print('用户分离函数')
    global g_total_num
    while (g_all_num < maxu and utype==0) or (len(g_related_uid_list) < maxu and utype==1) :
        tasks = []
        uid_list   = []
        # if random.randint(1,10) < 7 :
        #     print('<<<<<<<<<')
        #     uid_list =random.sample(range(1,50_0000), 100)
        # else:
        uid_list =random.sample(range(1,4_0000_0000), 100)
        # uid_list = [50911853]
        for uid in uid_list:
            if uid not in g_related_uid_list:
                if  (g_all_num >= maxu and utype==0) or (len(g_related_uid_list) >= maxu and utype==1) or g_stop:
                    return 
                tasks.append(fetch_user_favs_and_bvids(uid,maxt,tags_need))
                g_total_num += 1
                if(len(tasks)>=maxt):
                    await asyncio.gather(*tasks)
                    tasks=[]
        await asyncio.gather(*tasks)
#用户信息业务主线程
def user_info_thread(maxt,maxu,utype,tags_need):
    print('用户信息业务主线程')
    loop =  asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    a = asyncio.ensure_future(user_go(utype,maxu,maxt,tags_need))
    loop.run_until_complete(a)
    loop.close()
    return

def main():
    global g_stop
        
    if(os.path.exists(os.path.dirname(__file__)+'/cache.db')):
        os.remove(os.path.dirname(__file__)+'/cache.db')
    try:
        conn = sqlite3.connect(os.path.dirname(__file__)+'/cache.db')
        cursor = conn.cursor()
        cursor.execute('create table user (id integer primary key, owned_num integer, fav_num integer,  \
                                fav_related_num integer, owned_related_num integer,fav_related_bvids text,owned_related_bvids text,\
                                owned_bvids text, fav_bvids text,last_owned_time integer,last_fav_time integer)')
        cursor.execute('create table video (bvid text,tags text)')
    except BaseException as e:
        logging.warning(e) # 警告
    conn.close()

    sg.ChangeLookAndFeel('Topanga')
    base_layout = [ [sg.OptionMenu(('活跃用户', '目标用户'),size=(7,1),key='utype'),sg.Input(size=(8,1),default_text=100,key='maxu')], 
                    [sg.Text('线程：',size=(10,1)),sg.Input(size=(8,1),default_text=10,key='maxt')]]
    tag_layout = [[sg.MLine(default_text='英雄联盟\nLOL',enable_events=True,key=('tags'),size=(44,5),tooltip='标签不支持单引号和双引号')]]
    proxy_layout = [[sg.Text('没写',justification='center',  size=(18,1))]
                    ,[sg.Text(' ',justification='center',  size=(18,1))]]
    layout = [
        [sg.Frame('基本设置', base_layout,key='bf'), sg.Frame('代理设置 (总数:0)', proxy_layout,key='pf')],
        [sg.Frame('标签设置 (一行一个标签，当前共2个)', tag_layout,key='tt')],
        [sg.Text('用户进度\t(0/100)\t0% 活跃用户数:0',size=(42,1),key='upbt')],
        [sg.ProgressBar(100, orientation='h', size=(42, 20),key='upb')], 
        [sg.Button('<(￣︶￣)↗[GO!]',key='go'),sg.Text('\t耗时：',size=(28,1),key='cost')]
    ]
    w = sg.Window('BiliBili 爬虫',keep_on_top=True,alpha_channel=.9,disable_close=False).Layout(layout)

    while True:
        e, v = w.Read()
        if e == None:
            g_stop = True
            break
        elif e == 'go':
            start = time.time()
            w['go'].Update(disabled=True)
            tags_need = [v for v in v['tags'].split('\n') if v]
            utype = 0 if v['utype'] == '活跃用户' else 1
            maxt = int(v['maxt'])
            maxu = int(v['maxu'])
            thread_user_info = threading.Thread(target=user_info_thread, args=(maxt,maxu,utype,tags_need,), daemon=True)
            thread_user_info.start()
            threads.append(thread_user_info)
            
            while thread_user_info.is_alive():
                progress = g_all_num if utype==0 else len(g_related_uid_list)
                w['upbt'].Update(f'用户进度\t({progress}/{maxu})\t {round(progress*100/maxu)}% 活跃用户数:{g_all_num}')
                w['upb'].UpdateBar(progress,maxu)
                w['cost'].Update('总数:{}\t耗时：{}秒'.format(g_total_num,round(time.time()-start)))
                time.sleep(0.5)
            progress = g_all_num if utype==0 else len(g_related_uid_list)
            w['upbt'].Update(f'用户进度\t({progress}/{maxu})\t {round(progress*100/maxu)}% 活跃用户数:{g_all_num}')
            w['upb'].UpdateBar(progress,maxu)
            w['cost'].Update('总数:{}\t耗时：{}秒'.format(g_total_num,round(time.time()-start)))
            time.sleep(0.5)
            w['go'].Update('导出数据中.....')            
            time.sleep(0.5)
            db2_xls_main_go()
            w['go'].Update('<(￣︶￣)↗[GO!]')
            w['go'].Update(disabled=False)

        elif e=='tags':
            tags_need = [v for v in v['tags'].split('\n') if v]
            w['tt'].Update('标签设置 (一行一个标签，当前共{}个)'.format(len(tags_need)))
    w.close()

def sqlite_get_col_names(cur, table):
  query = 'select * from %s' % table
  cur.execute(query)
  return [tuple[0] for tuple in cur.description]
def sqlite_query(cur, table, col = '*', where = ''):
  if where != '':
    query = 'select %s from %s where %s' % (col, table, where)
  else:
    query = 'select %s from %s ' % (col, table)
  cur.execute(query)
  return cur.fetchall()


def sqlite_to_workbook(cur, table, workbook):
  ws = workbook.add_worksheet(table)
  print ('create table %s.' % table)
  if table =='related_user':
    ws.set_column('A:A', 10)
    ws.set_column('B:C', 14)
    ws.set_column('D:E', 18)
    ws.set_column('F:G', 12)
    ws.set_column('H:I', 18)
    ws.set_column('J:K', 20)
    bold = workbook.add_format({'bold': True})
    ws.set_row(0, 20, bold)
  elif table =='related_videos':
    ws.set_column('A:A', 18)
    bold = workbook.add_format({'bold': True})
    ws.set_row(0, 20, bold)
    ws.set_column('B:B', 150)



  for colx, heading in enumerate(sqlite_get_col_names(cur, table)):
      if heading=='id':
        ws.write(0,colx, '用户ID')
      elif heading =='owned_related_num':
        ws.write(0,colx, '发布相关视频数')
      elif heading =='fav_related_num':
        ws.write(0,colx, '收藏相关视频数')
      elif heading =='owned_related_bvids':
        ws.write(0,colx, '发布相关视频的BV号')
      elif heading =='fav_related_bvids':
        ws.write(0,colx, '收藏相关视频的BV号')
      elif heading =='owned_num':
        ws.write(0,colx, '发布视频总数')
      elif heading =='fav_num':
        ws.write(0,colx, '收藏视频总数')
      elif heading =='owned_bvids':
        ws.write(0,colx, '所有发布视频的BV号')
      elif heading =='fav_bvids':
        ws.write(0,colx, '所有收藏视频的BV号')
      elif heading =='last_owned_time':
        ws.write(0,colx, '最近发布视频时间')
      elif heading =='last_fav_time':
        ws.write(0,colx, '最近收藏视频时间')
      

      elif heading =='bvid':
        ws.write(0,colx, '视频BV号')
      elif heading =='tags':
        ws.write(0,colx, '视频标签')
      else:
        ws.write(0,colx, heading)

  for rowy,row in enumerate(sqlite_query(cur, table)):
    for colx, text in enumerate(row):
      if colx==9 or colx==10:
        timeStamp = text
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        ws.write(rowy+ 1, colx, otherStyleTime)
      else:
        ws.write(rowy+ 1, colx, text)
 
def db2_xls_main(dbpath):
  xlspath = dbpath[0:dbpath.rfind('.')] + '.xls'
  print ("<%s> --> <%s>"% (dbpath, xlspath))
  db = sqlite3.connect(dbpath)
  cur = db.cursor()
  workbook = xlsxwriter.Workbook(xlspath)     #新建excel表
 
  for tbl_name in [row[0] for row in sqlite_query(cur, 'sqlite_master', 'tbl_name', 'type = \'table\'')]:
    sqlite_to_workbook(cur,tbl_name, workbook)
  cur.close()
  db.close()
  workbook.close()

def db2_xls_main_go():
  # arg == database path
  log_path =os.path.dirname(__file__)+ '/cache.db'

  # main(sys.argv[1])
  db2_xls_main(log_path)

main()