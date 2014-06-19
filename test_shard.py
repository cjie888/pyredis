
from shard_redis import ShardJedis


serverInfos = ["10.18.105.119:6379", "10.18.105.119:6479"]
sharedJedis = ShardJedis(serverInfos)
for i in range(0,1000):
    key="key"+str(i)
    redis_client = sharedJedis.getRedis(key)
    #print redis_client
    print key,redis_client.get(key)
