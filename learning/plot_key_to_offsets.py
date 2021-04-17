import matplotlib.pyplot as plt


x = []
y = []
z = []
segments = []
m = []

# with open("/tmp/learnedDB/8.txt") as f:
#     lines = list(f.readlines())
#     for line in lines:
#         # print(line)
#         if (line.strip() == "Sizes"):
#             break 
#         segments.append(line.split())
#     for i in range(len(segments)):
#         segments[i][0] = int(segments[i][0])
#         segments[i][3] = float(segments[i][3])
#         segments[i][4] = float(segments[i][4])
#     print(segments[:5])
        

with open("/tmp/learnedDB/8.offsets") as f:
    lines = list(f.readlines())
    for line in lines:
        key, offset = line.split(" ")
        x.append(int(key))
        y.append(int(offset))
        print(line)
        for i in range(len(segments)-2, -1, -1):
            if segments[i][0] <= int(key):
                val = segments[i][3]*float(key) + segments[i][4]
                m.append(val)
                break


    # z.append(y[0])
    # for i in range(1, len(y)):
    #     z.append(y[i] - y[i-1])
    # x = x[:1000]
    # y = y[:1000]
    # m = m[:1000]
    # print(x)
    # print(y)
    plt.plot(x, y, linestyle = 'dotted')
    plt.plot(x, m, color='r')
    plt.xlabel("Key value")
    plt.ylabel("Offset (Bytes) in the file")
    plt.ticklabel_format(style = 'plain')
    plt.subplots_adjust(bottom=.25, left=.25)
    plt.savefig('plot.png')