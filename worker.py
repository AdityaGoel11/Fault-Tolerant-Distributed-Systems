import logging
import os
import re
import sys
from typing import Any
from base import Worker
from constants import FNAME, COUNT, IN
from mrds import MyRedis
import pandas as pd
import time,subprocess

try:
  class WcWorker(Worker):
    def run(self, **kwargs: Any) -> None:
      rds: MyRedis = kwargs['rds']
      processed_items = 0

      while True:
        try:
          message = rds.rds.xreadgroup(self.GROUP, self.pid, streams={IN: '>'}, count=1, block=1)
          
          if message:
            data = message[0][1][0][1]
            id = message[0][1][0][0]

          elif len(message) == 0:
            messages = rds.rds.xautoclaim(IN, self.GROUP, self.pid, 1000, count=1)
            if(messages[1]!=[]):
              data = messages[1][0][1]
              id = messages[1][0][0]
            else:
              continue
          logging.debug(f"Got {id} {data}")

        except:
          continue  

        fname: str = data[FNAME].decode()
        wc: dict[str, int] = {}

        for df in pd.read_csv(fname, chunksize=10000):
          all_text = ' '.join(df.iloc[:, 4])
          words = all_text.split(' ')

          for word in words:
              if word in wc:
                  wc[word] += 1
              else:
                  wc[word] = 1

        sorted_dict = dict(sorted(wc.items(), key=lambda item: item[1], reverse=True))

        # Take the top ten key-value pairs with the highest values
        top_10 = {k: sorted_dict[k] for k in list(sorted_dict)[:10]}
        rds.write(id,sorted_dict)   

        processed_items += 1

except:
  pass
  #print("redis down! printed from worker.")        
