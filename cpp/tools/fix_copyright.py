#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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