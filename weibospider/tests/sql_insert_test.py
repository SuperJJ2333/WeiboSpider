import json
import time

from sql.insert_sql import insert_user, insert_weibo

# 1. 读取数据
items = []
# 创建空列表，用于存储用户和微博数据
user_values_list = []
weibo_values_list = []
with open("../../output/tweet_spider_by_keyword_20230922142904.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        items.append(json.loads(line))

# 3. 插入数据
for item in items:
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
    if geo:
        geo = json.dumps(geo)
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

    # 将提取的数据追加到列表中
    user_values_list.append(user_values_args)
    weibo_values_list.append(weibo_values_args)

    insert_user(user_values_args)
    insert_weibo(weibo_values_args)

# # 执行批量插入
# insert_user(user_values_list)
# insert_weibo(weibo_values_list)

