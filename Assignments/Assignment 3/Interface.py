#!/usr/bin/python3
#
# INTerface for the assignement
#

import psycopg2
from itertools import islice
from io import StringIO

def getOpenConnection(user='postgres', password='root', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
        cur.execute("CREATE TABLE " + ratingstablename + "(userid INT NOT NULL, movieid INT, rating real, timestamp BIGINT);")
        with open(ratingsfilepath) as i:
            for n in iter(lambda: tuple(islice(i, 5000)), ()):
                batch = StringIO()
                write_string = ''
                for word in n:
                    write_string+=word.replace('::', ',')
                batch.write(write_string)
                batch.seek(0)
                cur.copy_from(batch, ratingstablename, sep=',',columns=('userid','movieid','rating','timestamp'))
        cur.execute("ALTER TABLE " + ratingstablename + " DROP timestamp")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    finally:
        if cur:
            cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        s = 5.0/numberofpartitions
        create1 = 'CREATE TABLE range_part{0} AS SELECT * FROM RATINGS WHERE rating>={1} and rating<={2}'
        create2 = 'CREATE TABLE range_part{0} AS SELECT * FROM RATINGS WHERE rating>{1} and rating<={2}'
        cur = openconnection.cursor()
        for i in range(numberofpartitions):
            if i == 0:
                cur.execute(create1.format(i, i*s, (i+1)*s))
            else:
                cur.execute(create2.format(i, i * s, (i + 1) * s))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    finally:
        if cur:
            cur.close()
    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        create3 = 'CREATE TABLE rrobin_part{0} AS SELECT userid,movieid,rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid FROM {1}) AS temp WHERE mod(temp.rowid-1,{2}) = {3}'
        cur = openconnection.cursor()
        for i in range(numberofpartitions):
            cur.execute(create3.format(i, ratingstablename, numberofpartitions, i))
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    finally:
        if cur:
            cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        
        cur.execute('''INSERT INTO {0} VALUES ({1},{2},{3})'''.format(ratingstablename, userid, itemid, rating))
        
        cur.execute('''SELECT * FROM {0} '''.format(ratingstablename))
        numofrecords = len(cur.fetchall())
        
        cur.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%' ''')
        numberofpartitions = len(cur.fetchall())

        tbid = (numofrecords-1)%numberofpartitions

        cur.execute('''INSERT INTO rrobin_part{0} VALUES ({1},{2},{3})'''.format(tbid, userid, itemid, rating))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    finally:
        if cur:
            cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute('''INSERT INTO {0} VALUES ({1},{2},{3})'''.format(ratingstablename, userid, itemid, rating))
        
        cur.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%' ''')
        numberofpartitions = len(cur.fetchall())

        insert = 'INSERT INTO range_part{0} VALUES ({1},{2},{3})'

        s = 5.0/numberofpartitions

        for i in range(numberofpartitions):
            if i==0:
                if rating>=i*s and rating<=(i+1)*s:
                    cur.execute(insert.format(i, userid, itemid, rating))
            else:
                if rating>i*s and rating<=(i+1)*s:
                    cur.execute(insert.format(i, userid, itemid, rating))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT ('Error %s' % e)
    finally:
        if cur:
            cur.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        prINT('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        prINT('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        prINT('Error %s' % e)
    finally:
        if cursor:
            cursor.close()