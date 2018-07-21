import os
import psutil

process = psutil.Process(os.getpid())
print_index = 0
print_every= 1000000

def __scale_bytes(number):
    kilo = 1024
    mega = kilo * 1024
    giga = mega * 1024
    value = 0
    factor = "byte"
    value = number
    if number > giga:
        value = number / giga
        factor = "gb"
    elif number > mega:
        value = number / mega
        factor = "mb"
    elif number > kilo:
        value = number / kilo
        factor = "kb"
    return "%s %s" % (round(value,2), factor)    

def __scale_number(number):
    k = 1000
    mil = k * 1000
    value = 0
    factor = ""
    value = number
    if number > mil:
        value = number / mil
        factor = "mil"
    elif number > k:
        value = number / k
        factor = "k"
    return "%s%s" % (round(value,2), factor)


def print_memory_usage():
    print("Memory Usage: %s" % __scale_bytes(process.memory_info().rss))

for temp_file in os.listdir("temp"):
    print("Creating Set for: %s" % temp_file)
    user_set = set()
    print_memory_usage()

    with open("temp/%s" % temp_file, "r") as followers_file:
        for line in followers_file.readlines():
            followers_batch = line.replace('\n', '').split(',')
            for user_id in followers_batch:
                user_set.add(user_id)
            if ( (len(user_set) / print_every) - print_index) >= 1:
                print("")
                print(__scale_number(len(user_set)))
                print_memory_usage()
                print_index += 1
        print("\n\n")
        print("File %s finished." % temp_file)
        print("Final memory used for the total set")
        print_memory_usage()

