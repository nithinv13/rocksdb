import matplotlib.pyplot as plt

x, table_reader, cache, pinned = [], [], [], []

with open("/tmp/learnedDB/memory_usage.csv") as f:
    lines = list(f.readlines())[1:]
    for i in range(len(lines)):
        line = lines[i].split(",")
        x.append(i)
        table_reader.append(line[0])
        cache.append(line[1])
        pinned.append(line[2])
    
    plt.plot(x, table_reader, color='r', label="Table reader memory")
    plt.plot(x, cache, color='b', label="Block cache usage")
    plt.plot(x, pinned, color='g', label="Pinned usage")
    plt.xlabel("No. of reads")
    plt.ylabel("Memory usage (B)")
    plt.xticks(x, [e*500 for e in x])
    plt.legend()
    plt.show()
