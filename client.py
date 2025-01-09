import glob
import logging
import os
import signal
import sys
import time
from threading import current_thread, Lock, Thread

from constants import LOGFILE, N_NORMAL_WORKERS, N_FILES, IS_RAFT, RAFT_CRASH_PORT, RAFT_PORTS, DATA_PATH, RAFT_JOIN_PORT
from mrds import MyRedis
from worker import WcWorker

workers = []

def checkpoints(rds):
  while True:
    try:
      if rds.is_pending():
        rds.checkpoint()
      else:
        break

    except:
      print(f"exception occurred in client checkpoint")
    time.sleep(1)


def sig_handler(signum, frame):
  for w in workers:
    w.kill()
  logging.info('Bye!')
  sys.exit()


if __name__ == "__main__":
  # Clear the log file
  open(LOGFILE, 'w').close()
  logging.basicConfig(# filename=LOGFILE,
                      level=logging.DEBUG,
                      force=True,
                      format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  thread = current_thread()
  thread.name = "client"
  logging.debug('Done setting up loggers.')

  # signal.signal(signal.SIGTERM, sig_handler)
  signal.signal(signal.SIGINT, sig_handler)

  for i in range(N_NORMAL_WORKERS):
    workers.append(WcWorker())

  # Wait for workers to finish processing all the files
  if(not IS_RAFT):
    try:
      lua_path = os.path.join(os.getcwd(),"mylib.lua")
      os.system(f"cat {lua_path} | redis-cli -a pass -x FUNCTION LOAD REPLACE")
      rds = MyRedis()
      for i, w in enumerate(workers):
        w.create_and_run(rds=rds)

      logging.debug('Created all the workers')
      for iter, file in enumerate(glob.glob(DATA_PATH)):
        rds.add_file(file)
      
      # TODO: Create a thread that creates a checkpoint every N seconds
      checkpoint_thread = Thread(target=checkpoints, args={rds})
      checkpoint_thread.start()
      
      while rds.is_pending():
        time.sleep(5)
        rds.restart(down_time=0.01, down_port=-1, instance_port=-1)

    except:
      rds.restart(down_time=0.01, down_port=-1, instance_port=-1)    

  elif(IS_RAFT):
    os.system(f"bash configure_redis.sh {' '.join(RAFT_PORTS)}")
    time.sleep(1)
    rds = MyRedis()
    for i, w in enumerate(workers):
      w.create_and_run(rds=rds, data_dir=DATA_PATH, workers_cnt=N_NORMAL_WORKERS, worker_id=i)

    time.sleep(2)
    rds.restart(down_time=1, down_port=RAFT_CRASH_PORT, instance_port=RAFT_JOIN_PORT)
    while rds.get_flag() != N_NORMAL_WORKERS:
      time.sleep(2)

  # Kill all the workers
  for w in workers:
    w.kill()

  # Wait for workers to exit
  while True:
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      break

  for word, c in rds.top(3):
    logging.info(f"{word.decode()}: {c}")
