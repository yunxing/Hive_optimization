import re
import sys

def getGroup(line):
    m = re.match("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^\"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^\"]*|\"[^\"]*\") (.*))", line)
    return m.groups()

def getNASAGroup(line):
    m = re.match(r"([^ ]*) ([^ ]*) ([^ ]*) (\-|\[[^\]]*\]) ([^\"]*|\".*?\") (-|[0-9]*) (-|[0-9]*)", line)    
    return m.groups()

def getClarkGroup(line):
    m = re.match(r"([^ ]*) ([^ ]*) ([^ ]*) (\-|\[[^\]]*\]) ([^\"]*|\".*?\") (-|[0-9]*) (-|[0-9]*)", line)
    return m.groups()

m = {};

input = open(sys.argv[1])
output = open(sys.argv[1]+".out","w")

for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    if not m.has_key(p[0]):
        m[p[0]] = 0
    m[p[0]] += 1
    if m[p[0]] <= 3:
        continue
    for s in p[0:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")

