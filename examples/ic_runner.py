import os

d = {}
d[1] = 2715810
d[2] = 51200 + 2715810
d[4] = 286720
d[5] = 286720

df = {}
df[1] = 9000624 
df[2] = 6331920
df[4] = 6567470
df[5] = 6567588

# for index in [1, 2, 4, 5]:
#     for block_cache_size in [0, 4, 6, 8, 10]:
#         command = "./index_comparer /mydata/learnedDB 5000000 " + str(block_cache_size*1024*1024) + " " + str(index) + " >> /mydata/rocksdb/examples/output.txt"
#         print(command)
#         os.system(command)

for index in [1, 2, 4, 5]:
    for table_cache_size in [100, 200, 400, 800, 1600, 3200]:
        if index == 2:
            block_cache_size = table_cache_size 
        else:
            block_cache_size = 0
        command = "./index_comparer /mydata/learnedDB 5000000 " + str(block_cache_size*1024) + " " + str(table_cache_size*1024) + " " + str(index) + " >> /mydata/rocksdb/examples/output.txt"
        print(command)
        os.system(command)