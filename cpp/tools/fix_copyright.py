#!/usr/bin/python
import os
import fnmatch

def checkAndFixFileHeaderComment(fileName, template, bazel=False):
    lines = []
    with open(fileName, 'r') as f:
        lines = f.readlines()

        if len(lines) > 0:
            if bazel:
                if lines[0].startswith("#"):
                    return False
            else:
                if lines[0].startswith("/*"):
                    return False
        
        lines.insert(0, template)
    
    with open(fileName, 'w') as f:
        for line in lines:
            f.write(line)
    return True


def main():
    bzl_template = "#" + os.linesep
    template = "/*" + os.linesep
    with open('tools/copyright.txt', 'r') as reader:
        line = reader.readline()
        while line != '':
           template += " * " + line
           bzl_template += "# " + line
           line = reader.readline()
    template += " */" + os.linesep
    bzl_template += "#" + os.linesep

    cnt = 0
    for root, dir, files in os.walk(os.curdir):
        if ".git/" in root:
            continue

        for item in fnmatch.filter(files, "*.h"):
            if(checkAndFixFileHeaderComment(root + os.sep + item, template)):
                cnt = cnt +1

        for item in fnmatch.filter(files, "*.cpp"):
            if (checkAndFixFileHeaderComment(root + os.sep + item, template)):
                cnt = cnt + 1

        for item in fnmatch.filter(files, "*.bazel"):
            if (checkAndFixFileHeaderComment(root + os.sep + item, bzl_template, bazel=True)):
                cnt = cnt + 1

    print("{} files fixed".format(cnt))


if __name__ == "__main__":
    main()