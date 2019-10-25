from sys import argv, platform
from os import walk, path
from time import sleep
import hashlib
from argparse import ArgumentParser, FileType, ArgumentDefaultsHelpFormatter
from multiprocessing import Process, Manager, cpu_count, freeze_support, Queue
from datetime import datetime


def time(for_file=False):
    if for_file:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S - ")


######################################################################################
SUPPORTED_HASHES = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256
}


######################################################################################
class DoHash:
    def __init__(self, types):
        global SUPPORTED_HASHES
        self.T = []
        for x in types:
            if x in SUPPORTED_HASHES:
                self.T.append(SUPPORTED_HASHES[x]())

    def update(self, data):
        [x.update(data) for x in self.T]

    def get(self):
        return [x.hexdigest() for x in self.T]


######################################################################################
def get_hash(q_f: Queue, q_o: Queue, hash_types):
    while True:
        file = q_f.get()
        if not file:
            break
        h = DoHash(hash_types)
        try:
            size = path.getsize(file)
            with open(file, "rb") as f:
                while True:
                    data = f.read(1024 * 1024 * 20)
                    h.update(data)
                    if f.tell() >= size:
                        break
                q_o.put([file, str(size)] + h.get())
        except Exception as e:
            q_o.put([file, -1, str(e)])


######################################################################################
def write_out(out_file, q_o: Queue, hash_types):
    with open(out_file, "w") as f:
        f.write(";".join(["file", "size(B)"] + hash_types) + "\n")
        while True:
            data = q_o.get()
            if not data:
                break
            f.write(";".join(data) + "\n")


######################################################################################
def list_str(values):
    return values.split(',')


######################################################################################
def main():
    ##########################################
    parser = ArgumentParser(
        prog='hash_all_files',
        description='Create hash for all files in specific directory',
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'output_file',
        metavar='output file',
        type=FileType('w', encoding='UTF-8'),
        help='Output file')
    parser.add_argument(
        '--dir',
        metavar='start directory',
        type=str,
        default="c:\\",
        nargs='?',
        help='Start directory')
    parser.add_argument(
        '--hash',
        type=str,
        default="md5, sha1",
        help='Select hash functions [' + ', '.join(SUPPORTED_HASHES) + ']')
    parser.add_argument(
        '--w',
        type=int,
        default=0,
        help='Set maximum of workers 0 = same as number of cores')
    args = parser.parse_args()
    # print(args)
    ##########################################
    p = path.abspath(args.dir)
    number_of_cpu = cpu_count()
    if path.isdir(p):
        g_path = p
    else:
        print(time(), "Invalid folder:", p)
        parser.print_help()
        return
    ##########################################
    change = []
    for x in args.hash.replace(", ", "\n").replace(",", "\n").replace(" ", "\n").split("\n"):
        #print(x)
        if x in SUPPORTED_HASHES:
            change.append(x)
    if len(change) > 0:
        hashes_types = change
    else:
        print("Hash not recognized", args.hash)
        return
    ##########################################
    print("*" * 50)
    print("Starting with:")
    print("Outupu file:", path.abspath(args.output_file.name))
    print("Path:", g_path)
    print("Used hashes:", ", ".join(hashes_types))
    if 0 < args.w < number_of_cpu:
        print("Num of workers:", args.w)
    else:
        print("Num of workers:", number_of_cpu)
    print("*" * 50)
    ##########################################
    print(time(), "Prepare workers")
    manager = Manager()
    q_files = manager.Queue()
    q_output = manager.Queue()
    worker = Process(target=write_out, args=(path.abspath(args.output_file.name), q_output, hashes_types))
    if 0 < args.w < number_of_cpu:
        process = [Process(target=get_hash, args=(q_files, q_output, hashes_types)) for _ in range(args.w)]
    else:
        process = [Process(target=get_hash, args=(q_files, q_output, hashes_types)) for _ in range(number_of_cpu)]
    print(time(), "Init workers")
    worker.start()
    worker.join(0.1)
    [x.start() for x in process]
    [x.join(0.1) for x in process]
    print(time(), "Start walking")
    ##########################################
    for directory, _, files in walk(g_path):
        for file in files:
            file = path.join(directory, file)
            q_files.put(file)
    ##########################################
    print(time(), "Walking end")
    sleep(1)
    print(time(), "Send end signal to workers")
    [q_files.put(False) for _ in process]
    ##########################################
    while True:
        if not any([x.is_alive() for x in process]):
            break
        sleep(1)
    [x.join() for x in process]
    print(time(), "Workers ended")
    ##########################################
    q_output.put(False)
    print(time(), "Finish writings")
    worker.join()
    print(time(), "End")


######################################################################################
if __name__ == '__main__':
    if platform.startswith('win'):
        # On Windows calling this function is necessary.
        freeze_support()
    main()
######################################################################################
