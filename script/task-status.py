import sys
import requests

URL = sys.argv[1]

response = requests.get(URL)

if 'application/json' in response.headers['Content-Type']:
    result = response.json()
else:
    result = response.text

tasks = []
flg = False
for line in result.split('\n'):
    if line.strip() == "var attemptsTableData=[":
        flg = True
        continue

    if not flg:
        continue

    if line[0] == ']':
        flg = False
        continue

    line = line[1:-1].split(',')
    line[0] = '-'.join(line[0].split('>')[1].split('<')[0].split('_')[4:])
    line[1] = float(line[1][1:-1])
    line[2] = line[2]
    line[3] = "status"
    line[4] = line[4].split('>')[1].split('<')[0].split(':')[0]

    tasks.append(line[:5])

for line in tasks:
    print(line)
