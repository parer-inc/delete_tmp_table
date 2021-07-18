"""This service allows to delete temporary tables in db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()


def delete_tmp_table(name):
    """Deletes tmp table"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    if "tmp" not in name:
        # log that name was wrong
        return False
    try:
        cursor.execute(f"""DROP TABLE `{name}`""")
    except MySQLdb.Error as error:
        print(error)
        # LOG
        return False
        # sys.exit("Error:Failed to create new tmp table")
    db.commit()
    cursor.close()
    return True


if __name__ == '__main__':
    q = Queue('delete_tmp_table', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r, name='delete_tmp_table')
        worker.work()
