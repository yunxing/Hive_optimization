import os
import sys
import re
def cut(sizeInMB, inputFileName):
    sizeInBytes = int(sizeInMB) * 1024 * 1024
    print "maximum size:" + str(sizeInBytes)
    input = open(inputFileName)
    outputFileName = "" + str(sizeInMB) + "." + inputFileName
    output = open(outputFileName, "w")
    for line in input:
#        if re.match(r"((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)", line) != None:
        output.write(line)
        if os.path.getsize(outputFileName) > sizeInBytes:
            break
    print "finishing size:" + str(os.path.getsize(outputFileName))

inputFileName = sys.argv[1] 
for i in range(5, 50, 4):
    cut (i, inputFileName)
