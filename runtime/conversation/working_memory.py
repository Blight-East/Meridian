import json, redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def update_working_memory(user_id, key, value):
    mem_key = f"operator_memory:{user_id}"
    mem = r.get(mem_key)
    mem_dict = json.loads(mem) if mem else {}
    mem_dict[key] = value
    r.set(mem_key, json.dumps(mem_dict), ex=86400) # 24h expiration

def get_working_memory(user_id):
    mem_key = f"operator_memory:{user_id}"
    mem = r.get(mem_key)
    return json.loads(mem) if mem else {}
