import json
import redis
import asyncio
from redis.sentinel import Sentinel
from redis.exceptions import ConnectionError, TimeoutError

async def get_current_data(_redis_server, _port, _key, _dbase):
    # get current data set
    r = redis.Redis(host=_redis_server, port=_port, db=_dbase)
    current_rec = r.get(_key)
    return current_rec

def get_master_node(_tuple_list, _master_name, _timeout=5):
    try:
        # determine master node
        sentinel = Sentinel(_tuple_list)
        # get master ip and port
        master_ip, master_port = sentinel.discover_master(_master_name)
        # check master tcp socket
        master_conn = sentinel.master_for(_master_name, socket_timeout=_timeout)
        master_conn.ping()
        if master_conn.ping():
            return master_ip, master_port
        return True
    except (ConnectionError, TimeoutError) as _err:
        print(_err)


def check_duplicate(_redis_server, _port, _key, _dbase, _hostname):
    # check for existing data in current data set
    r = redis.Redis(host=_redis_server, port=_port, db=_dbase)
    _records = r.get(_key)
    _data = json.loads(_records)
    _status = "no_duplicate_records"

    for _datum in _data:
        if _hostname == _datum['host']:
            _status = "duplicate_record"
    return _status

if __name__ == "__main__":

    try:

        # servers running redis and redis-sentinel
        sentinel_tuple_list = [('redis-1', 26379), ('redis-2', 26379), ('redis-3', 26379)]
        # redis-sentinel master name
        master_name = "xlabs"
        # get redis-sentinel master ip and tcp port
        redis_host_ip, redis_host_port = get_master_node(sentinel_tuple_list, master_name)
        # redis key and database name
        main_key = "systems"
        database = 1
        # dictionary and user variable of new database entries
        new_hosts = { "new": ["artemis.xmondo.com","hera.xmondo.com"]}
        new_user = "xmondo"

        # iterate over dictionary and check for duplicates
        for new_host in new_hosts["new"]:
            status = check_duplicate(redis_host_ip, redis_host_port, main_key, database, new_host)
            if status == "duplicate_record":
                # skip duplicates
                continue
            else:
                # update new entry
                records = asyncio.run(get_current_data(redis_host_ip, redis_host_port, main_key, database))
                if records is None:
                    exit()
                else:
                    records = asyncio.run(get_current_data(redis_host_ip, redis_host_port, main_key, database))
                    data = json.loads(records)
                    new_system = {
                        "host": "%s" % new_host,
                        "user": "%s" % new_user
                    }
                    data.append(new_system)
                    r_conn = redis.Redis(host=redis_host_ip, port=redis_host_port, db=database)
                    r_conn.set(main_key, json.dumps(data))

    except Exception as err:
        print(err)