import os
try:
    import psycopg2
except:
    print('Cannot find psycopg2')
import configparser
from tabulate import tabulate

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'settings.cfg')
config.read(config_file)
cfg_database = config['PostgreSQL']['database']
cfg_user = config['PostgreSQL']['user']
cfg_password = config['PostgreSQL']['password']
cfg_host = config['PostgreSQL']['host']
cfg_port = config['PostgreSQL']['port']


def connect():
    try:
        connection = psycopg2.connect(
            database=cfg_database,
            user=cfg_user,
            password=cfg_password,
            host=cfg_host,
            port=cfg_port
        )
    except psycopg2.OperationalError as e:
        print('ERROR: Cannot connect to database')
        print('{message}'.format(message=str(e)))
        exit()
    cursor = connection.cursor()
    return connection, cursor


def disconnect(connection, cursor):
    cursor.close()
    connection.close()


def select(s, d):
    conn, cur = connect()
    cur.execute(s, d)
    rows = cur.fetchall()
    conn.commit()
    disconnect(conn, cur)
    return rows


def insert(s, d):
    conn, cur = connect()
    try:
        cur.execute(s, d)
    except psycopg2.Error as e:
        print('ERROR: {message}'.format(message=str(e)))
        exit()
    try:
        res = conn.commit()
        return res
    except psycopg2.Error as e:
        print('ERROR: {message}'.format(message=str(e)))
        exit()
    disconnect(conn, cur)


def update(s, d):
    conn, cur = connect()
    cur.execute(s, d)
    conn.commit()
    disconnect(conn, cur)


def delete(s, d):
    conn, cur = connect()
    cur.execute(s, d)
    conn.commit()
    disconnect(conn, cur)


def printSQL(s, d):
    conn, cur = connect()
    cur.execute(s, d)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    conn.commit()
    disconnect(conn, cur)

    table = []
   
    for row in rows:
        r = []
        for col in row:
            r.append(col)
        print(r)
        table.append(r)
    print(tabulate(table, headers=colnames, tablefmt="fancy_grid"))

def setOrderStatus(o, s):
    SQL = "UPDATE orders set status = %s where ordernumber = %s"
    data = (s, o)
    update(SQL, data)


def setImageStatus(self, o, f, s):
    SQL = "UPDATE images set status = %s where ordernumber = %s AND file_name = %s"
    data = (s, o, f)
    update(SQL, data)


def ordercomplete(o):
    conn, cur = connect()
    cur.callproc("ordercomplete", [o, ])
    r = bool(cur.fetchall()[0][0])
    conn.commit()
    disconnect(conn, cur)
    return r
