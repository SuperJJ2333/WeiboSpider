from db_conn import db_conn


def insert_weibo(weibo_values_args):
    sql = """
    INSERT INTO weibos (weibo_id, mblogid, created_at, geo, ip_location, reposts_count,
                            comments_count, attitudes_count, source, content, pic_urls,
                            pic_num, isLongText, url, keyword, crawl_time, user_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    db_conn(sql).employment_mysql_insert(weibo_values_args)

    return


def insert_user(user_values_args):
    sql = """
    INSERT INTO users (user_id, nick_name, verified, mbrank, mbtype)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    nick_name = VALUES(nick_name),
    verified = VALUES(verified),
    mbrank = VALUES(mbrank),
    mbtype = VALUES(mbtype)
    """

    db_conn(sql).employment_mysql_insert(user_values_args)

    return
