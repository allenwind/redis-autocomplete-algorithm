import redis
import bisect
import uuid

class AutoComplete:

    def __init__(self, store_size=100):
        self._conn = redis.Redis()
        self._store_size = store_size

    def add(self, key, value):
        ac_list = 'recent:' + key
        pipeline = self._conn.pipeline(True)
        pipeline.lrem(ac_list, value)
        pipeline.lpush(ac_list, value)
        pipeline.ltrim(ac_list, 0, self._store_size-1)
        pipeline.execute()

    def remove(self, key, value):
        self._conn.lrem('recent:'+key, value)

    def find_prefix(self, key, prefix):
        candidates = self._conn.lrange('recent:'+key, 0, -1)
        matches = []
        for candidate in candidates:
            if candidate.lower().decode('utf-8').startswith(prefix):
                matches.append(candidate)
        return matches

class ZAutoComplete:

    characters = '`abcdefghijklmnopqrstuvwxyz{'

    def __init__(self, extract_size=10):
        self._conn = redis.Redis()
        self._size = extract_size

    def add(self, key, value):
        self._conn.zadd('members:'+key, value, 0)

    def remove(self, key, value):
        self._conn.zrem('members:'+key, value)

    def find_prefix_range(self, prefix):
        local = bisect.bisect_left(self.characters, prefix[-1:])
        suffix = self.characters[(local or 1) - 1]
        _id = str(uuid.uuid4())
        # _id = ''
        return prefix[:-1] + suffix + '{' + _id, prefix + '{' + _id

    def find_prefix(self, key, prefix):
        start, end = self.find_prefix_range(prefix)
        key = 'members:' + key
        self._conn.zadd(key, start, 0, end, 0)
        pipeline = self._conn.pipeline(True)
        while True:
            try:
                pipeline.watch(True)
                start_index = pipeline.zrank(key, start)
                end_index = pipeline.zrank(key, end)
                size = min(start_index+self._size-1, end_index-2)
                pipeline.multi()
                pipeline.zrem(key, start, end)
                pipeline.zrange(key, start_index, end_index)
                items = pipeline.execute()[-1]
                break
            except redis.exceptions.WatchError:
                continue
        return [item for item in items if b'{' not in item]
                
        
def test_AutoComplete():
    ac = AutoComplete()
    ac.add('allen', 'wind')
    ac.add('allen', 'windy')
    ac.add('allen', 'winding')
    print(ac.find_prefix('allen', 'wind'))
    ac.remove('allen', 'winding')
    print(ac.find_prefix('allen', 'wind'))

def test_ZAutoComplete():
    zac = ZAutoComplete()
    zac.add('allen', 'wind')
    zac.add('allen', 'windy')
    zac.add('allen', 'winding')
    print(zac.find_prefix('allen', 'wind'))
    zac.remove('allen', 'winding')
    print(zac.find_prefix('allen', 'wind'))

if __name__ == '__main__':
    test_AutoComplete()
    test_ZAutoComplete()
