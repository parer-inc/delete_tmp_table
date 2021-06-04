"""This service allows to create temporary tables in db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor


def delete_tmp_table(name):
    """Create new tmp table"""
    cursor, db = get_cursor()
    if "tmp" not in name:
        # log that name was wrong
        return False
    try:
        cursor.execute(f"""CREATE TABLE {name}
                        'id' int AUTO_INCREMENT,
                        'data' varchar(50),
                       """)
    except MySQLdb.Error as error:
        print(error)
        sys.exit("Error:Failed to create new tmp table")
    db.commit()
    cursor.close()
    return True


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('delete_tmp_table', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r, name='delete_tmp_table')
        worker.work()
