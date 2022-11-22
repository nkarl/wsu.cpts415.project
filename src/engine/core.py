# pip install joblib
import os
os.system("echo 'Adding python package joblib for parallelization...'")
os.system("python -m pip install joblib")

# https://stackoverflow.com/questions/9786102/how-do-i-parallelize-a-simple-python-loop
import time
from joblib import Parallel, delayed

def countdown(n):
    while n>0:
        n -= 1
    return n


t = time.time()
for _ in range(20):
    print(countdown(10**7), end=" ")
print(time.time() - t)  


t = time.time()
results = Parallel(n_jobs=-1)(delayed(countdown)(10**7) for _ in range(20))
print(results)
print(time.time() - t)
