import sys
import re

reg = re.compile("(?P<url>https?://[^\s]+)")
cur_page = "NONE"
ur_c = {}

for inp in sys.stdin:
  if (inp[0:4] == "http" and len(inp.split(" ")) == 5):
    cur_page = inp.split(" ")[0]
    for (url, count) in ur_c.items():
      print "%s\t%s\t%s" % (cur_page, url, count)
    ur_c = {}

  for url in reg.findall(inp):
     ur_c[url] = ur_c.get(url, 0) + 1
