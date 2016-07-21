# -*- coding:utf-8 -*-
#
import mysql.connector
import threading
import functools
import logging
import exceptions

class _Engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self.connect()
engine = None

#用来创建一个连接数据库代理
def create_engine(user, password, database, host='127.0.0.1', port = 3306, **kw):
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))
    global engine
    engine = _Engine(lambda: mysql.connector.connect(**params))

class DBError(exceptions.AttributeError):
    pass

class _DbContxt(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        """
        返回一个布尔值，用于判断 此对象的初始化状态
        """
        return self.connection is not None

    def init(self):
        """
        初始化连接的上下文对象，获得一个惰性连接对象
        """
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        """
        清理连接对象，关闭连接
        """
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        """
        获取cursor对象， 真正取得数据库连接
        """
        return self.connection.cursor()

class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            self.connection = engine.connect()
            logging.info('connect to db')
        return self.connection.cursor()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' % hex(id(connection)))
            _connection.close()

    def commit(self):
        if self.connection:
            self.connection.commit()

_dbContxt = _DbContxt()

class _Connect_txt(object):
    def __enter__(self):
        global _dbContxt
        self.should_cleanup = False
        if not _dbContxt.is_init():
            _dbContxt.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _dbContxt
        if self.should_cleanup:
            _dbContxt.cleanup()

#define a class,for can get or set a attibute like, a.key = value / xx = a.key
class _Dict(dict):
    def __init__(self, names, values, **args):
        super(_Dict,self).__init__(**args)
        for name, value in zip(names,values):
            self[name] = value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError('dict has no attribute %s' %name)

    def __setattr__(self, name, value):
            self[name] = value


def connection():
    return _Connect_txt()

def with_connect(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with _Connect_txt():
            return func(*args, **kwargs)
    return wrapper


def update(sql, *args):
    return _update(sql, *args)

@with_connect
def _update(sql, *args):
    global _dbContxt
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _dbContxt.connection.cursor()
        cursor.execute(sql, *args)
        r = cursor.rowcount
        if _dbContxt.transactions == 0:
            # no transaction enviroment:
            logging.info('auto commit')
            _dbContxt.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()
#执行select方法
@with_connect
def select(sql, *args):
    global _dbContxt
    cursor = None
    sql = sql.replace('?','%s')
    try:
        cursor = _dbContxt.cursor()
        cursor.execute(sql,*args)
        if cursor.description:
            column_name = [x[0] for x in cursor.description]
            values = [x for x in cursor.fetchall()]
            return [_Dict(column_name,value) for value in values]
    finally:
        if cursor:
            cursor.close()



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root','admin','test','127.0.0.1',3306)
    with connection():
        update('insert into user (id, name) values (%s, %s)', ['77', 'Mike13'])
        print select('select * from user')
