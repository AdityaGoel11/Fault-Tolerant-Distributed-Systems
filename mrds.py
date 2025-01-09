from __future__ import annotations

import logging
from typing import Optional, Final
import subprocess
from redis.client import Redis
import time

from base import Worker
from constants import IN, COUNT, FNAME, IS_RAFT, RAFT_PORTS, RAFT_CRASH_PORT, RAFT_JOIN_PORT
import json,os

class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password="",
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)
    script_file = os.path.join(os.path.dirname(__file__), 'mylib.lua')
    with open(script_file, 'r') as f:
        self.lua_script = f.read()

  def get_timestamp(self) -> float:
    timestamp = self.rds.time()
    return float(f'{timestamp[0]}.{timestamp[1]}')

  def add_file(self, fname: str) -> None:
    self.rds.xadd(IN, {FNAME: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)

  def get_latency(self) -> list[float]:
    lat = []
    lat_data = self.rds.hgetall("latency")
    for k in sorted(lat_data.keys()):
      v = lat_data[k]
      lat.append(float(v.decode()))
    return lat

  def read(self, worker: Worker) -> Optional[tuple[bytes, dict[bytes, bytes]]]:
      return -1

  def write(self, id: bytes, wc: dict[str, int]) -> None: 
      while True:
        try:   
          self.rds.fcall("add_wc",0,json.dumps(wc),Worker.GROUP,id)
          break
        except:
          time.sleep(0.01)  
      return -1

  def is_pending(self):
    pending = self.rds.xpending(IN, Worker.GROUP)
    if(pending['pending']==0): return False
    else: return True

  def checkpoint(self):
     self.rds.bgsave() 
     print(f"Checkpointing!")
     return -1

  def restart(self, down_time, down_port, instance_port):
    if(not IS_RAFT):
      password = "12345678"
      stop_cmd = f'echo "{password}" | sudo -S systemctl stop redis-server'
      subprocess.run(stop_cmd,shell=True, check=False)
      time.sleep(down_time)

      restart_cmd = f'echo "{password}" | sudo -S systemctl restart redis-server'
      subprocess.run(restart_cmd, shell=True, check=True)
      print(f"Redis restarted!")