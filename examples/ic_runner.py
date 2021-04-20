import os

DIRECTORY = "/mydata/learnedDB"

def get_bounds():
    files = os.listdir(DIRECTORY)
    for file_name in files:
        if file_name.endswith(".offsets"):
            if file_name.replace(".offsets", ".sst") in files:
                with open(os.path.join(DIRECTORY, file_name)) as f:
                    lines = list(f.readlines())
                    print()
                    print(int(lines[-1].split()[0]) - int(lines[1].split()[0]), file_name)

def get_SL_errors():
    segments = []
    for file_name in os.listdir(DIRECTORY):
        if file_name.endswith(".txt"):
            with open(os.path.join(DIRECTORY, file_name)) as f:
                line = list(f.readlines())
                segments.append((line[0], file_name))
    
    errors = []
    for entry in segments:
        error = (int(entry[0].split()[2]), entry[1])
        errors.append(error)

    errors.sort()
    # median_error = errors[len(errors)//2]
    # tail_percentile = errors[(int)(len(errors)*0.95)]
    for error in errors:
        print(error)

    # print(median_error, tail_percentile)

def get_offset_diff(file_name):
    x, y = [], []
    with open(os.path.join(DIRECTORY, file_name)) as f:
        lines = list(f.readlines())[1:]
        for line in lines:
            a, b = line.split()
            x.append(int(a))
            y.append(int(b))

    diff = []
    for i in range(1, len(x)):
        diff.append(abs(x[i]-x[i-1]))

    for i in range(len(diff)-1):
        print(x[i], x[i+1], diff[i])
  
# d = {}
# d[1] = 2715810
# d[2] = 51200 + 2715810
# d[4] = 286720
# d[5] = 286720

# df = {}
# df[1] = 9000624 
# df[2] = 6331920
# df[4] = 6567470
# df[5] = 6567588

# for index in [1, 2, 4, 5]:
#     for block_cache_size in [0, 4, 6, 8, 10]:
#         command = "./index_comparer /mydata/learnedDB 5000000 " + str(block_cache_size*1024*1024) + " " + str(index) + " >> /mydata/rocksdb/examples/output.txt"
#         print(command)
#         os.system(command)

def run_exps():
    os.system("rm nohup.out")
    database_sizes = [10000000, 15000000, 20000000]
    database_sizes = [5000000]
    for database_size in database_sizes:
        # j = (int)(database_size / 5000000)
        table_cache_sizes = [str(i*100*1024) for i in range(96, 100, 8)]
        for index in [5]:
            for table_cache_size in table_cache_sizes:
                if index == 2:
                    block_cache_size = table_cache_size 
                    table_cache_size = 200*1024
                else:
                    block_cache_size = 0
                command = "./index_comparer /mydata/learnedDB " + str(database_size) + " " + str(block_cache_size) + " " + str(table_cache_size) + " " + str(index) + " >> /mydata/rocksdb/examples/output.txt"
                print(command)
                os.system(command)

if __name__ == "__main__":
    get_SL_errors()
    # run_exps()
    # get_bounds()
    # get_offset_diff("004660.offsets")