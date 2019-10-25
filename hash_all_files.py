from sys import argv, platform
from os import walk, path
from time import sleep
import hashlib
from argparse import ArgumentParser, FileType, ArgumentDefaultsHelpFormatter
from multiprocessing import Process, Manager, cpu_count, freeze_support
from datetime import datetime
######################################################################################x
def time(for_file = False):
	if for_file:
		return datetime.now().strftime("%Y%m%d_%H%M%S")
	return datetime.now().strftime("%Y-%m-%d %H:%M:%S - ")
######################################################################################
SUPPORTED_HASHS =  {
		"md5": hashlib.md5,
		"sha1": hashlib.sha1,
		"sha256": hashlib.sha256
		}
######################################################################################
class DoHash():
	def __init__(self, types):
		global SUPPORTED_HASHS
		self.T = []
		for x in types:
			if x in SUPPORTED_HASHS:
				self.T.append(SUPPORTED_HASHS[x]())
	def update(self,data):
		[x.update(data) for x in self.T]
	def get(self):
		return [x.hexdigest() for x in self.T]
######################################################################################
def get_hash(Qf, Qo, hash_types):
	while True:
		file = Qf.get()
		if not file:
			break
		h = DoHash(hash_types)
		try:
			size = path.getsize(file)
			with open(file, "rb") as f:
				while True:
					data = f.read(1024*1024*20)
					h.update(data)
					if f.tell() >= size:
						break
				Qo.put([file, str(size)] + h.get())
		except Exception as e:
			Qo.put([file, str(size), str(e)])
######################################################################################
def write_out(out_file, Qo, hash_types):
	with open(out_file,"w") as f:
		f.write(";".join(["file","size(B)"] + hash_types) + "\n")
		while True:
			data = Qo.get()
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
		type=FileType('w',encoding='UTF-8'), 
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
		help='Select hash functions ['+', '.join(SUPPORTED_HASHS)+']')
	args = parser.parse_args()
	print(args)
	##########################################
	p = path.abspath(args.dir)
	if path.isdir(p):
		PATH = p
	else:
		print(time(),"Invalid folder:", p)
		parser.print_help()
		return
	##########################################
	change = []
	for x in args.hash.replace(", ","\n").replace(",","\n").replace(" ","\n").split("\n"):
		print(x)
		if x in SUPPORTED_HASHS:
			change.append(x)
	if len(change) > 0:
		HAHS_TYPE = change
	else:
		print("Hash not recognized", args.hash)
		return
	##########################################
	print("*"*50)
	print("Starting with:")
	print("Outupu file:",path.abspath(args.output_file.name))
	print("Path:", PATH)
	print("Used hashes:",", ".join(HAHS_TYPE))
	print("*"*50)
	##########################################
	print(time(),"Prepare workers")
	M = Manager()
	Qf = M.Queue()
	Qo = M.Queue()
	W = Process(target=write_out, args=(path.abspath(args.output_file.name), Qo, HAHS_TYPE))
	P = [Process(target=get_hash, args=(Qf,Qo,HAHS_TYPE)) for x in range(cpu_count())]
	print(time(),"Init workers")
	W.start()
	W.join(0.1)
	[x.start() for x in P]
	[x.join(0.1) for x in P]
	print(time(),"Start walkings")
	##########################################
	for directory, _, files in walk(PATH):
		for file in files:
			file = path.join(directory, file)
			Qf.put(file)
	##########################################
	print(time(),"Wolk end")
	sleep(1)
	print(time(),"Send end signal to workers")
	for x in P:
		Qf.put(False)
	##########################################
	while True:
		if not any([x.is_alive() for x in P]):
			break
		sleep(1)
	[x.join() for x in P]
	print(time(),"Workers ended")
	##########################################
	Qo.put(False)
	print(time(),"Finish writings")
	W.join()
	print(time(),"End")
######################################################################################
if __name__ == '__main__':
	if platform.startswith('win'):
		# On Windows calling this function is necessary.
		freeze_support()
	main()
######################################################################################