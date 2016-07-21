import mysql.connector


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

conn = mysql.connector.connect(user = 'root', password = 'admin', database = 'test', use_unicode = True)
cursor  = conn.cursor()

#cursor.execute('insert into user (id, name) values (%s, %s)', ['2', 'Bob'])
#print cursor.rowcount


conn.commit()

cursor = conn.cursor()
cursor.execute('select * from user')

if cursor.description:
    column_name = [x[0] for x in cursor.description]
    values = [x for x in cursor.fetchall()]
    print [_Dict(column_name,value) for value in values]




