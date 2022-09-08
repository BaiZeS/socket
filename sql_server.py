import pymssql


def connect_sqlserver():
    # 连接数据库
    try:
        conn = pymssql.connect(
            host='139.9.147.35',
            user='sa',
            password='scwj.123',
            database='test_db'
            # database='wjwatersystem',
        )
    except:
        print('error in connecting to sqlserver')
    return conn


def close_sql(conn):
    conn.close()    # 关闭数据库连接


def query_sql(conn, sql):
    '''
    定义查询:
    conn:数据库对象; sql:查询语句;
    '''
    cursor = conn.cursor()  # 建立游标
    cursor.execute(sql) # 执行查询语句
    data = cursor.fetchall()    # 获取查询结果
    cursor.close()  # 关闭游标
    return data


def execute_sql(conn, sql, data=()):
    '''
    定义写入：
    conn:数据库对象; sql:插入语句; num:插入多条数据，默认为否;
    '''
    cursor = conn.cursor()  # 建立游标
    try:
        if len(data):
            cursor.executemany(sql, data)    # 插入多条数据
        else:
            cursor.execute(sql)  # 插入单条数据
        conn.commit()     # 执行插入语句
    except Exception as e:
        print('process of insert into sqlserver error: ', repr(e))
        conn.rollback()   # 回滚操作
    finally:
        cursor.close()      # 关闭游标