import sys, subprocess

nodes = {f"192.168.0.11{i}":0 for i in range(5)}

if len(sys.argv) <= 1:
    print("Usage: python3 block-report.py [filepath]", file=sys.stderr)
    exit()
filepath = sys.argv[1]

fsck = subprocess.Popen(['hdfs', 'fsck', filepath, '-files' ,'-blocks', '-locations'],\
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
stdout, stderr = fsck.communicate()
for line in stdout.decode("utf-8").split('\n'):
    if len(line) == 0: continue
    if '0' <= line[0] <= '9':
	    block = line.split('DatanodeInfoWithStorage[')
	    nodes[block[1].split(':')[0]] += 1
	    nodes[block[2].split(':')[0]] += 1

for node, num in nodes.items():
    print(f"{node} \t {num}")
