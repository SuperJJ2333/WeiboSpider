import pymysql as mysql

mydb = mysql.connect(
    host="gz-cynosdbmysql-grp-g8clt7yv.sql.tencentcdb.com",  # 数据库主机地址
    port=20014,  # 端口号
    user="lin",  # 数据库用户名
    passwd="Lin2225427",  # 数据库密码
    database="employment_schema"  # 选择一个数据库
)

mycursor = mydb.cursor()

######新建一个表######
try:
    mycursor.execute("CREATE TABLE sites (name VARCHAR(255), url VARCHAR(255))")
except:
    print("已经存在这个表了")

#########显示所有表######
mycursor.execute("SHOW TABLES")
for x in mycursor:
    print(x)
