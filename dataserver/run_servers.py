import sys
import subprocess
import time


def circle_offset(array, start_index):
	return array[start_index:] + array[:start_index]


def main(start_port, num_processes):
	ports = [str(i) for i in range(start_port, start_port + num_processes)]
	processes = []
	for i in xrange(num_processes):
		args = ["python", "main.py"] + circle_offset(ports, i)
		processes.append(subprocess.Popen(args))
		print "Started on PID", processes[-1].pid, args

	try:
		while True:
			time.sleep(100)
	finally:
		for i in processes:
			i.terminate()

if __name__ == "__main__":
	main(int(sys.argv[1]), int(sys.argv[2]))
