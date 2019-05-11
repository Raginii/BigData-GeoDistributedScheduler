import subprocess
import sys
from sys import stderr
from optparse import OptionParser
import os
import time
import datetime
import re
import multiprocessing
from cacher import *
from hive_benchmark  import *

def run_hive_benchmark(opts):
  def ssh_hive(command, user="root"):
    command = 'sudo -u %s %s' % (user, command)
    print command
    ssh(opts.hive_host, "root", opts.hive_identity_file, command)

  def clear_buffer_cache_hive(host):
    print >> stderr, "Clearing", host
    ssh(host, "root", opts.hive_identity_file,
        "sudo bash -c \"sync && echo 3 > /proc/sys/vm/drop_caches\"")

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  slaves_file_name = "%s_slaves" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  local_slaves_file = os.path.join(LOCAL_TMP_DIR, slaves_file_name)
  query_file = open(local_query_file, 'w')
  remote_result_file = "/mnt/%s_results" % prefix
  remote_tmp_file = "/mnt/%s_out" % prefix
  remote_query_file = "/mnt/%s" % query_file_name

  query_list = "set mapreduce.reduce.input.limit = -1; set mapred.reduce.tasks = %s; " % opts.reduce_tasks
  query_list += "DROP TABLE IF EXISTS scratch_rank;"
  query_list += "CREATE TABLE scratch_rank AS SELECT pageURL, pageRank FROM scratch WHERE pageRank > 1000;"

  # Throw away query for JVM warmup
  # query_list += "SELECT COUNT(*) FROM scratch;"

  if '4' not in opts.query_num:
    query_list += CLEAN_QUERY
  else:
    opts.query_num = '4_HIVE'

  query_list += query_map[opts.query_num][0]

  query_list = re.sub("\s\s+", " ", query_list.replace('\n', ' '))

  print "\nQuery:"
  print query_list.replace(';', ";\n")

  query_file.write(
    "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))

  query_file.write(
      "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s\n" % (
        remote_tmp_file, remote_result_file))

  query_file.close()

  print "Copying query files to Hive host"
  scp_to(opts.hive_host, opts.hive_identity_file, "root", local_query_file,
      remote_query_file)
  ssh_hive("chmod 775 %s" % remote_query_file)

  # Run benchmark
  print "Running remote benchmark..."

  # Collect results
  results = []
  contents = []

  for i in range(opts.num_trials):
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    if opts.clear_buffer_cache:
      print >> stderr, "Clearing Buffer Cache..."
      map(clear_buffer_cache_hive, opts.hive_slaves)
    ssh_hive("%s" % remote_query_file)
    local_results_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
    scp_from(opts.hive_host, opts.hive_identity_file, "root",
        "/mnt/%s_results" % prefix, local_results_file)
    content = open(local_results_file).readlines()
    all_times = map(lambda x: float(x.split(": ")[1].split(" ")[0]), content)

    if '4' in opts.query_num:
      query_times = all_times[-4:]
      part_a = query_times[1]
      part_b = query_times[3]
      print "Parts: %s, %s" % (part_a, part_b)
      result = float(part_a) + float(part_b)
    else:
      result = all_times[-1] # Only want time of last query

    print "Result: ", result
    print "Raw Times: ", content

    results.append(result)
    contents.append(content)

    # Clean-up
    #ssh_hive("rm /mnt/%s*" % prefix)
    print "Clean Up...."
    ssh_hive("rm /mnt/%s_results" % prefix)
    os.remove(local_results_file)

  os.remove(local_query_file)

  return results, contents
