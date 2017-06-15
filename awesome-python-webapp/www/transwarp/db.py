# coding=utf-8

import time
import uuid
import threading
import functools
import logging
import psycopg2


# 全局数据库引擎
engine = None


def next_id(t=None):
    """
    生成一个唯一id 由 当前时间 + 随机数（伪随机数）拼接
    """
    if t is None:
        t = time.time()
    return "%015d%s000" % (int(t * 1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
    """
    用于记录sql的执行时间
    """
    t = time.time() - start
    if t > 0.1:
        logging.warning("[PROFILING] [DB] %s: %s" % (t, sql))
    else:
        logging.info("[PROFILING] [DB] %s: %s" % (t, sql))


def DBError(Exception):
    pass


def MultiColumsError(DBError):
    pass


class Dict(dict):
    """
    """

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" & key)

    def __setattr__(self, key, value):
        self[key] = value


# 数据可以引擎对象
class _Engine(object):
    """
    数据库引擎对象
    用于保存 db 模块的核心函数：
    create_engine 创建出来的数据库连接
    """

    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


def create_engine(user, password, database, host='127.0.0.1', port=5432, db_type="postgres", **kw):
    """
    db模型的核心函数，用于连接数据库，生成全局对象engine，
    engine对象持有数据库连接
    """
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password,
                  database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8',
                    collation='utf8_general_ci', autocommit=False)

    

    if db_type == 'postgres':
        engine = _Engine(lambda: psycopg2.connect(**params))
        # test connection...
        logging.info('Init postgres engine <%s> ok.' % hex(id(engine)))

    
    if db_type == 'mysql':
        # 去掉不规范的设置
        for k, v in defaults.iteritems():
            params[k] = kw.pop(k, v)

        # 补上没有设置的值
        params.update(kw)
        params['buffered'] = True
        
        #import mysql.connector
        #engine = _Engine(lambda: mysql.connector.connect(**params))
        # test connection...
        logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))
    


class _LazyConnection(object):
    """
    惰性连接对象
    仅当需要cursor对象时，才连接数据库，获取连接
    """

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            logging.info('[CONNECTION] [OPEN] connection <%s>...' %
                         hex(id(_connection)))
            self.connection = _connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' %
                         hex(id(connection)))
            _connection.close()

# 数据库的上下文对象


class _DbCtx(threading.local):
    """
    db模块的核心对象, 数据库连接的上下文对象，负责从数据库获取和释放连接
    取得的连接是惰性连接对象，因此只有调用cursor对象时，才会真正获取数据库连接
    该对象是一个 Thread local对象，因此绑定在此对象上的数据 仅对本线程可见
    """

    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        """
        返回一个布尔值， 用于判断 此对象的初始化状态
        """
        return not self.connection is None

    def init(self):
        """
        初始化连接的上下文对象， 获得一个惰性连接对象
        """
        self.connection = _LazyConnection()
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


#_db_ctx是threadlocal对象，
# 所以，它持有的数据库连接对于每个线程看到的都是不一样的。
# 任何一个线程都无法访问到其他线程持有的数据库连接。
_db_ctx = _DbCtx()


# 连接上下文对象
class _Connection(object):
    """
    __enter__()和__exit__()的对象可以用于with语句，
    确保任何情况下__exit__()方法可以被调用。
    """

    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup


def connection():
    """
    db 模块核心函数，用于获取一个数据库连接
    通过 _ConnectionCtx 对 _db_ctx封装， 使得惰性连接可以自动
    获取和释放。
    可以使用with语法处理数据库连接
    """
    return _Connection()


def with_connection(func):
    """
    对 with 包装一下
    """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        with connection():
            return func(*args, **kwargs)
    return wrapped


class _TransactionCtx(object):
    """
    事务嵌套比Connect嵌套复杂一点，因为事务嵌套需要计数，
    每遇到一层嵌套就+1，离开一层嵌套就-1，最后到0时候提交事务
    """

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            # 打开连接
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions += 1
        logging.info('begin transaction...' if _db_ctx.transactions ==
                     1 else "join current transaction...")
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions -= 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info("commit transation...")
        try:
            _db_ctx.connection.commit()
            logging.info("commit ok.")
        except:
            logging.warning("commit failed. try rollback...")
            _db_ctx.connection.rollback()
            logging.warning('roll')

    def rollback(self):
        global _db_ctx
        logging.warning("rollback transaction...")
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')


def transaction():
    """
    db模块核心函数 用于实现事物功能
    支持：
        with db.transaction():
            db.select("...")
            db.update("...")
            db.update("...")
    支持事物嵌套：
        with db.transaction():
            transaction1
            transaction2
            ...
    """
    return _TransactionCtx()


def with_transaction(func):
    """
    设计一个装饰器 替换with语法
    比如：
        @with_transaction
        def do_in_transaction():
            pass

    >>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select_one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        with transaction():
            return func(*args, **kwargs)
    return wrapped


def insert(table, **kw):
    """
    执行insert语句
    """
    cols, args = zip(*kw.iteritems())
    sql = "insert into '%s' (%s) values (%s)" % (table, ','.join(
        ["'%s'" % col for col in cols]), ','.join(["?" for i in range(len(cols))]))


@with_connection
def _select(sql, first, *args):
    """
    执行SQL，返回一个结果 或者多个结果组成的列表
    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchone()]
    finally:
        if cursor:
            cursor.close()

def select_one(sql, *args):
    """
    执行SQL 仅返回一个结果
    如果没有结果 返回None
    如果有1个结果，返回一个结果
    如果有多个结果，返回第一个结果
    >>> u1 = dict(id=100, name='Alice', email='alice@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> u2 = dict(id=101, name='Sarah', email='sarah@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> u = select_one('select * from user where id=?', 100)
    >>> u.name
    u'Alice'
    >>> select_one('select * from user where email=?', 'abc@email.com')
    >>> u2 = select_one('select * from user where passwd=? order by email', 'ABC-12345')
    >>> u2.name
    u'Alice'
    """
    return _select(sql, True, *args)

def select_int(sql, *args):
    """
    执行一个sql 返回一个数值，
    注意仅一个数值，如果返回多个数值将触发异常
    >>> u1 = dict(id=96900, name='Ada', email='ada@test.org', passwd='A-12345', last_modified=time.time())
    >>> u2 = dict(id=96901, name='Adam', email='adam@test.org', passwd='A-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> select_int('select count(*) from user')
    5
    >>> select_int('select count(*) from user where email=?', 'ada@test.org')
    1
    >>> select_int('select count(*) from user where email=?', 'notexist@test.org')
    0
    >>> select_int('select id from user where email=?', 'ada@test.org')
    96900
    >>> select_int('select id, name from user where email=?', 'ada@test.org')
    Traceback (most recent call last):
        ...
    MultiColumnsError: Expect only one column.
    """
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MultiColumsError('Expect only one colum.')
    return d.values()[0]

def select(sql, *args):
    """
    执行sql 以列表形式返回结果
    >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> L = select('select * from user where id=?', 900900900)
    >>> L
    []
    >>> L = select('select * from user where id=?', 200)
    >>> L[0].email
    u'wall.e@test.org'
    >>> L = select('select * from user where passwd=? order by id desc', 'back-to-earth')
    >>> L[0].name
    u'Eva'
    >>> L[1].name
    u'Wall.E'
    """
    return _select(sql, False, *args)


@with_connection
def _update(sql, *args):
    """
    执行update语句，返回update的行数
    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', "%s")
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            # 非事务
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def update(sql, *args):
    """
    执行update语句， 返回update的行数
    """
    return _update(sql, *args)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    create_engine("postgres", "1129", "test")
    update('drop table if exists "user"')
    update('create table "user" (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()
