#!/usr/bin/python3
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    final_rult = []
    #opening the connection
    cursor = openconnection.cursor()

    part_query = 'SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'.format(ratingMinValue, ratingMaxValue)
    cursor.execute(part_query)
    all_partitions = cursor.fetchall()
    all_partitions = [partition[0] for partition in all_partitions]

    range_select_query = 'SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'

    for partition in all_partitions:
        cursor.execute(range_select_query.format(partition, ratingMinValue, ratingMaxValue))
        sql_rult = cursor.fetchall()
        for r in sql_rult:
            r = list(r)
            r.insert(0,'RangeRatingsPart{}'.format(partition))
            final_rult.append(r)

    rr_count_query = 'SELECT partitionnum FROM roundrobinratingsmetadata;'
    cursor.execute(rr_count_query)
    rr_parts = cursor.fetchall()[0][0]

    rr_select_query = 'SELECT * FROM roundrobinratingspart{0} WHERE rating>={1} and rating<={2};'

    for i in range(0,rr_parts):
        cursor.execute(rr_select_query.format(i, ratingMinValue, ratingMaxValue))
        sql_rult = cursor.fetchall()
        for r in sql_rult:
            r = list(r)
            r.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            final_rult.append(r)

    writeToFile('RangeQueryOut.txt', final_rult)
    pass



def PointQuery(ratingsTableName, ratingValue, openconnection):
    final_rult = []
    cursor = openconnection.cursor()

    part_query = 'SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={0};'.format(ratingValue)
    cursor.execute(part_query)
    all_partitions = cursor.fetchall()
    all_partitions = [partition[0] for partition in all_partitions]

    range_select_query = 'SELECT * FROM rangeratingspart{0} WHERE rating={1};'

    for partition in all_partitions:
        cursor.execute(range_select_query.format(partition, ratingValue))
        sql_rult = cursor.fetchall()
        for r in sql_rult:
            r = list(r)
            r.insert(0, 'RangeRatingsPart{}'.format(partition))
            final_rult.append(r)

    rr_count_query = 'SELECT partitionnum FROM roundrobinratingsmetadata;'

    cursor.execute(rr_count_query)
    rr_parts = cursor.fetchall()[0][0]

    rr_select_query = 'SELECT * FROM roundrobinratingspart{0} WHERE rating={1};'

    for i in range(0, rr_parts):
        cursor.execute(rr_select_query.format(i, ratingValue))
        sql_rult = cursor.fetchall()
        for r in sql_rult:
            r = list(r)
            r.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            final_rult.append(r)

    writeToFile('PointQueryOut.txt', final_rult)
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()