import os

for index in [1, 2, 4, 5]:
    for block_cache_size in [0, 200, 400, 600, 800]:
        command = "./index_comparer /mydata/learnedDB 5000000 " + str(block_cache_size*1024*1024) + " " + str(index) + " >> /mydata/rocksdb/examples/output.txt"
        print(command)
        os.system(command)