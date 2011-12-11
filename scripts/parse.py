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
output = open(sys.argv[1]+".out","w")

input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")
input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")
input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")
input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")
input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")
input = open(sys.argv[1])
for line in input:
    try:
        p = getNASAGroup(line)
    except:
        print line
        break
    output.write(p[0] + "")
    for s in p[1:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")    
