import pyhash
import redis


hasher = pyhash.murmur2_x64_64a()

'''''
    this is a shard redis which transplant from jedis(redis java client)
    Example:
        serverInfos = ["10.18.105.119:6379", "10.18.105.119:6479"]
        sharded_redis = ShardJedis(serverInfos)
        key="test"
        sharded_redis.delete(key)
'''
class ShardJedis():

    def __init__(self,redisServerStrs):

        self.nodesMap = {}
        for i in range(len(redisServerStrs)):
            for n in range(160):
                hashKey = "SHARD-" + str(i) + "-NODE-" + str(n)
                mapKeyHash = self.getHash(hashKey);
                mapKeyHash = self.changePyLong2JavaLong(mapKeyHash)
                self.nodesMap[mapKeyHash] = redisServerStrs[i]

    '''''
    change py long value to java long
    '''
    def changePyLong2JavaLong(self,pyLong):
        if(pyLong > 9223372036854775807):#max long value in java
            pyLong = (pyLong+2**63)%2**63 - 2**63
        return pyLong;

    '''''
    get hash value by murmur2_x64_64a
    '''
    def getHash(self,key):
        hashCode =  hasher(key,seed=0x1234ABCD)
        hashCode = self.changePyLong2JavaLong(hashCode)
        return hashCode


    def getShardInfo(self,key):
        hashKey = self.getHash(key)
        nodeKeys = self.nodesMap.keys()
        nodeKeys.sort()
        resultKey = nodeKeys[0]
        #print nodeKeys
        for nodeKey in nodeKeys:
            if(nodeKey >= hashKey):
                resultKey = nodeKey
                break
        return self.nodesMap.get(resultKey)

    '''''
    get redis client by key
    '''
    def getRedis(self,key):
        redisInfo = self.getShardInfo(key)
        redisInfos = redisInfo.split(":")
        #print redisInfo
        return redis.StrictRedis(host = redisInfos[0], port = int(redisInfos[1]))

    def delete(self, key):
        redis_client = self.getRedis(key)
        return redis_client.delete(key)

    def set(self, key, value):
        redis_client = self.getRedis(key)
        return redis_client.set(key,value)

    def get(self, key):
        redis_client = self.getRedis(key)
        return redis_client.get(key)

    def exists(self, key):
        redis_client = self.getRedis(key)
        return redis_client.exists(key)

    def expire(self, key, seconds):
        redis_client = self.getRedis(key)
        return redis_client.expire(key, seconds)

    def expireat(self, key, when):
        redis_client = self.getRedis(key)
        return redis_client.expireat(key, when)

    def ttl(self, key):
        redis_client = self.getRedis(key)
        return redis_client.ttl(key)

    def lrange(self, key, start, end):
        redis_client = self.getRedis(key)
        return redis_client.lrange(key, start, end)

    def incr(self, key, amount=1):
        redis_client = self.getRedis(key)
        return redis_client.incr(key, amount)

    def getset(self, key, newvalue):
        redis_client = self.getRedis(key)
        return redis_client.getset(key, newvalue)

    def llen(self, key):
        redis_client = self.getRedis(key)
        return redis_client.llen(key)

    def hincrby(self, key, hash_key, amount=1):
        redis_client = self.getRedis(key)
        return redis_client.hincrby(key,hash_key, amount)

    def zrevrange(self, key, start, end):
        redis_client = self.getRedis(key)
        return redis_client.zrevrange(key, start, end)

def main():
        serverInfos = ["127.0.0.1:6379", "127.0.0.1:6479"]
        sharded_redis = ShardJedis(serverInfos)
        key="test"
        sharded_redis.delete(key)