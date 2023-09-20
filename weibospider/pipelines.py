# -*- coding: utf-8 -*-
import datetime
import json
import os.path
import time
import logging
import pymysql

from db_conn import db_conn
from sql.insert_sql import insert_user, insert_weibo


class JsonWriterPipeline(object):
    """
    写入json文件的pipline
    """

    def __init__(self):
        self.file = None
        if not os.path.exists('../output'):
            os.mkdir('../output')

    def process_item(self, item, spider):
        """
        处理item
        """
        if not self.file:
            now = datetime.datetime.now()
            file_name = spider.name + "_" + now.strftime("%Y%m%d%H%M%S") + '.jsonl'
            self.file = open(f'../output/{file_name}', 'wt', encoding='utf-8')
        item['crawl_time'] = int(time.time())
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        self.file.flush()
        return item


class MySqlWriterPipeline(object):

    def process_item(self, item, spider):

        # 提取item中的数据
        user = item['user']
        user_id = user['_id']
        nick_name = user['nick_name']
        verified = user['verified']
        mbrank = user['mbrank']
        mbtype = user['mbtype']

        weibo_id = item['_id']
        mblogid = item['mblogid']
        created_at = item['created_at']
        geo = item['geo']
        ip_location = item['ip_location']
        reposts_count = item['reposts_count']
        comments_count = item['comments_count']
        attitudes_count = item['attitudes_count']
        source = item['source']
        content = item['content']
        pic_urls = ','.join(item['pic_urls']) if item['pic_urls'] else None
        pic_num = item['pic_num']
        isLongText = item['isLongText']
        url = item['url']
        keyword = item['keyword']
        crawl_time = int(time.time())

        user_values_args = (user_id, nick_name, verified, mbrank, mbtype)

        weibo_values_args = (
            weibo_id, mblogid, created_at, geo, ip_location, reposts_count,
            comments_count, attitudes_count, source, content, pic_urls,
            pic_num, isLongText, url, keyword, crawl_time, user_id)

        # 执行sql语句
        insert_user(user_values_args)
        insert_weibo(weibo_values_args)

        # print("SQL 正在运行")
        # 添加日志输出
        spider.logger.info(f"SQL processed: {item}")

        return item

