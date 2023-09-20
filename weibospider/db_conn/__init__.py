import pandas as pd
import pymysql
import psycopg2
import time


class db_conn:
    def __init__(self, sql, is_write=False):
        """
        数据库连接类，用于执行SQL语句，并记录SQL执行信息。

        参数：
            sql (str): 要执行的SQL语句。
            is_write (bool, optional): 是否为写入操作。默认为False，表示执行读取操作。

        属性：
            sql (str): 要执行的SQL语句。
            st_timestamp (float): SQL执行开始时间戳。
            sql_output (Any): SQL执行结果。如果为读取操作，通常为DataFrame，如果为写入操作，通常为None。
            is_write (bool): 是否为写入操作的标志。
            write_status (Any): 写入操作的状态信息。

        注意：当 is_write 为 True 时，执行写入操作，并且需要进行事务管理；当 is_write 为 False 时，执行读取操作。
        """
        # sql属性存储要执行的SQL语句。
        self.sql = sql
        # st_timestamp属性存储SQL执行开始时间戳，以便后续计算SQL执行时间。
        self.st_timestamp = time.time()
        # sql_output属性存储SQL执行结果。如果为读取操作，通常为DataFrame类型，如果为写入操作，通常为None。
        self.sql_output = None
        # 是否写入，区别在于一个直接调用pd的read_sql，另外一个通过传统的创建游标的方法，主要因为需要进行事务管理
        self.is_write = is_write
        # 写入状态
        self.write_status = None

    def spend_seconds(self):
        """
        打印查询成功信息，包括查询结果的行数和执行耗时。

        输出：
            打印查询成功信息，格式为 "查询成功，共<行数>行数据, 共耗时: <耗时>s"。
        """
        # 使用 len() 函数获取 SQL 执行结果的行数，并打印查询成功信息
        print(F'查询成功，共{len(self.sql_output)}行数据, 共耗时: {(time.time() - self.st_timestamp): .1f}s')

    def select_sql(self, conn_type):
        """
        执行SQL查询操作，并根据指定的数据库连接类型获取查询结果。

        参数：
            conn_type (Any): 数据库连接类型，可以是数据库连接对象或其他适当的数据库连接类型。

        输出：
            将查询结果存储在属性 self.sql_output 中，并调用 spend_seconds 方法打印查询成功信息。
            self.sql_output格式为DataFrame
        """
        try:
            # 通过指定的数据库连接类型 conn_type 执行 SQL 查询操作，并将查询结果存储在 self.sql_output 中
            self.sql_output = pd.read_sql(self.sql, conn_type)
            # 调用 spend_seconds 方法，打印查询成功信息（行数和执行耗时）
            self.spend_seconds()
        except Exception as e:
            # 如果发生异常，打印查询错误信息
            print(F"查询错误：{str(e)}")
        finally:
            # 不论是否发生异常，最后关闭数据库连接 conn_type
            conn_type.close()

    def insert_sql(self, conn_type, values_args):
        """
        执行SQL插入操作，并记录写入状态。

        参数：
            conn_type (Any): 数据库连接类型，可以是数据库连接对象或其他适当的数据库连接类型。
            values_args (Any): 插入的数据，可以是单个数据项或多个数据项的列表。

        输出：
            无输出，执行插入操作，并将写入状态存储在属性 self.write_status 中。
        """
        # 创建游标
        cursor = conn_type.cursor()
        try:
            # 判断是否有多个数据项需要插入
            if isinstance(values_args, tuple) or isinstance(values_args, list) and len(values_args) > 1:
                # 使用 executemany() 方法批量插入数据
                cursor.execute(self.sql, values_args)
            else:
                # 使用 execute() 方法插入单个数据项
                cursor.execute(self.sql)
            # 提交事务
            conn_type.commit()
            # 设置写入状态为“完成”
            self.write_status = 'finished'
            print("写入成功！")
        except Exception as e:
            # 写入失败，回滚事务
            conn_type.rollback()
            # 设置写入状态，并包含异常信息
            print(F"写入失败：{e}")
            self.write_status = F'failed: {str(e)}'
        finally:
            # 关闭游标
            cursor.close()

    def employment_mysql_query(self):
        """
        连接数据库，查询数据库中的数据
        """
        # 定义数据库连接的主机名
        employment_mysql_host = 'gz-cynosdbmysql-grp-g8clt7yv.sql.tencentcdb.com'
        # 定义数据库连接的用户名
        employment_mysql_user = 'lin'
        # 定义数据库连接的密码
        employment_mysql_password = 'Lin2225427'
        # 定义数据库连接的端口号
        employment_mysql_port = 20014
        # 定义数据库连接的数据库名
        employment_mysql_db = 'employment_schema'
        # 创建 employment 数据库连接
        employment_mysql_conn = pymysql.connect(host=employment_mysql_host, user=employment_mysql_user,
                                                password=employment_mysql_password, port=employment_mysql_port,
                                                db=employment_mysql_db, charset="utf8mb4")
        # 运行sql
        self.select_sql(employment_mysql_conn)
        return self.sql_output

    def employment_mysql_insert(self, values_args):

        employment_mysql_host = 'gz-cynosdbmysql-grp-g8clt7yv.sql.tencentcdb.com'
        employment_mysql_user = 'lin'
        employment_mysql_password = 'Lin2225427'
        employment_mysql_port = 20014
        employment_mysql_db = 'employment_schema'
        # 创建 employment 数据库连接
        employment_mysql_conn = pymysql.connect(host=employment_mysql_host, user=employment_mysql_user,
                                                password=employment_mysql_password, port=employment_mysql_port,
                                                db=employment_mysql_db, charset="utf8mb4")
        # 运行sql
        self.insert_sql(employment_mysql_conn, values_args)
