import re
def getGroup(line):
    m = re.match("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^\"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^\"]*|\"[^\"]*\") (.*))", line)
    return m.groups()

input = open("a.log")
output = open("output.log","w")

for line in input:
    p = getGroup(line)
    for s in p[0:-1]:
        output.write(s+"")
    output.write(p[-1] + "\n")

