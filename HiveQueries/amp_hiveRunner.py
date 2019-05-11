import subprocess
import sys
from sys import stderr
from optparse import OptionParser
import os
import time
import datetime
import re
import multiprocessing

from StringIO import StringIO

# A scratch directory on your filesystem
LOCAL_TMP_DIR = "/tmp"

### Benchmark Queries ###
TMP_TABLE = "result"
TMP_TABLE_CACHED = "result_cached"
CLEAN_QUERY = "DROP TABLE %s;" % TMP_TABLE

# TODO: Factor this out into a separate file
QUERY_1a_HQL = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000"
QUERY_1b_HQL = QUERY_1a_HQL.replace("1000", "100")
QUERY_1c_HQL = QUERY_1a_HQL.replace("1000", "10")

QUERY_2a_HQL = "SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM " \
                 "uservisits GROUP BY SUBSTR(sourceIP, 1, 8)"
QUERY_2b_HQL = QUERY_2a_HQL.replace("8", "10")
QUERY_2c_HQL = QUERY_2a_HQL.replace("8", "12")

QUERY_3a_HQL = """SELECT sourceIP,
                          sum(adRevenue) as totalRevenue,
                          avg(pageRank) as pageRank
                   FROM
                     rankings R JOIN
                     (SELECT sourceIP, destURL, adRevenue
                      FROM uservisits UV
                      WHERE UV.visitDate > "1980-01-01"
                      AND UV.visitDate < "1980-04-01")
                      NUV ON (R.pageURL = NUV.destURL)
                   GROUP BY sourceIP
                   ORDER BY totalRevenue DESC
                   LIMIT 1"""
QUERY_3a_HQL = " ".join(QUERY_3a_HQL.replace("\n", "").split())
QUERY_3b_HQL = QUERY_3a_HQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_HQL = QUERY_3a_HQL.replace("1980-04-01", "2010-01-01")

QUERY_4_HQL = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS
                   SELECT TRANSFORM (line)
                   USING "python /root/url_count.py" as (sourcePage,
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS
                   SELECT SUM(count) AS totalCount, destpage
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL = " ".join(QUERY_4_HQL.replace("\n", "").split())

QUERY_4_HQL_HIVE_UDF = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS
                   SELECT TRANSFORM (line)
                   USING "python /tmp/url_count.py" as (sourcePage,
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS
                   SELECT SUM(count) AS totalCount, destpage
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL_HIVE_UDF = " ".join(QUERY_4_HQL_HIVE_UDF.replace("\n", "").split())

QUERY_1_PRE = "CREATE TABLE %s (pageURL STRING, pageRank INT);" % TMP_TABLE
QUERY_2_PRE = "CREATE TABLE %s (sourceIP STRING, adRevenue DOUBLE);" % TMP_TABLE
QUERY_3_PRE = "CREATE TABLE %s (sourceIP STRING, " \
    "adRevenue DOUBLE, pageRank DOUBLE);" % TMP_TABLE

QUERY_1a_SQL = QUERY_1a_HQL
QUERY_1b_SQL = QUERY_1b_HQL
QUERY_1c_SQL = QUERY_1c_HQL

QUERY_2a_SQL = QUERY_2a_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2b_SQL = QUERY_2b_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2c_SQL = QUERY_2c_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_3a_SQL = """SELECT sourceIP, totalRevenue, avgPageRank
                    FROM
                      (SELECT sourceIP,
                              AVG(pageRank) as avgPageRank,
                              SUM(adRevenue) as totalRevenue
                      FROM Rankings AS R, UserVisits AS UV
                      WHERE R.pageURL = UV.destinationURL
                      AND UV.visitDate
                        BETWEEN Date('1980-01-01') AND Date('1980-04-01')
                      GROUP BY UV.sourceIP)
                   ORDER BY totalRevenue DESC LIMIT 1""".replace("\n", "")
QUERY_3a_SQL = " ".join(QUERY_3a_SQL.replace("\n", "").split())
QUERY_3b_SQL = QUERY_3a_SQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_SQL = QUERY_3a_SQL.replace("1980-04-01", "2010-01-01")

def create_as(query):
  return "CREATE TABLE %s AS %s;" % (TMP_TABLE, query)
def insert_into(query):
  return "INSERT INTO TABLE %s %s;" % (TMP_TABLE, query)
def count(query):
  return query
  return "SELECT COUNT(*) FROM (%s) q;" % query

QUERY_MAP = {
             '1a':  (create_as(QUERY_1a_HQL), insert_into(QUERY_1a_HQL),
                     create_as(QUERY_1a_SQL)),
             '1b':  (create_as(QUERY_1b_HQL), insert_into(QUERY_1b_HQL),
                     create_as(QUERY_1b_SQL)),
             '1c':  (create_as(QUERY_1c_HQL), insert_into(QUERY_1c_HQL),
                     create_as(QUERY_1c_SQL)),
             '2a': (create_as(QUERY_2a_HQL), insert_into(QUERY_2a_HQL),
                    create_as(QUERY_2a_SQL)),
             '2b': (create_as(QUERY_2b_HQL), insert_into(QUERY_2b_HQL),
                    create_as(QUERY_2b_SQL)),
             '2c': (create_as(QUERY_2c_HQL), insert_into(QUERY_2c_HQL),
                    create_as(QUERY_2c_SQL)),
             '3a': (create_as(QUERY_3a_HQL), insert_into(QUERY_3a_HQL),
                    create_as(QUERY_3a_SQL)),
             '3b': (create_as(QUERY_3b_HQL), insert_into(QUERY_3b_HQL),
                    create_as(QUERY_3b_SQL)),
             '3c': (create_as(QUERY_3c_HQL), insert_into(QUERY_3c_HQL),
                    create_as(QUERY_3c_SQL)),
             '4':  (QUERY_4_HQL, None, None),
             '4_HIVE':  (QUERY_4_HQL_HIVE_UDF, None, None)}

def parse_args():
  parser = OptionParser(usage="run_query.py [options]")
  parser.add_option("--hive", action="store_true", default=False,
      help="Whether to include Hive")
  parser.add_option("--hive-host",
      help="Hostname of Hive master node")
  parser.add_option("--hive-slaves",
      help="Hostnames of Hive slaves (comma seperated)")
  parser.add_option("-q", "--query-num", default="1a",
                    help="Which query to run in benchmark: " \
                    "%s" % ", ".join(QUERY_MAP.keys()))

  (opts, args) = parser.parse_args()

  if not (opts.impala or opts.shark or opts.redshift or opts.hive or opts.hive_cdh):
    parser.print_help()
    sys.exit(1)

  if opts.hive or opts.hive_cdh:
    opts.hive_slaves = opts.hive_slaves.split(",")
    print >> stderr, "Hive slaves:\n%s" % "\n".join(opts.hive_slaves)

  if opts.query_num not in QUERY_MAP:
    print >> stderr, "Unknown query number: %s" % opts.query_num
    sys.exit(1)

  return opts

def ssh(host, username, identity_file, command):
  return subprocess.check_call(
      "ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
      (identity_file, username, host, command), shell=True)

def scp_to(host, identity_file, username, local_file, remote_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (identity_file, local_file, username, host, remote_file), shell=True)

# Copy a file to a given host through scp, throwing an exception if scp fails
def scp_from(host, identity_file, username, remote_file, local_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
      (identity_file, username, host, remote_file, local_file), shell=True)


def get_percentiles(in_list):
  def get_pctl(lst, pctl):
    return lst[int(len(lst) * pctl)]
  in_list = sorted(in_list)
  return "%s\t%s\t%s" % (
    get_pctl(in_list, 0.05),
    get_pctl(in_list, .5),
    get_pctl(in_list, .95)
  )


def main():
  global opts
  opts = parse_args()

  results, contents = run_hive_benchmark(opts)

  output = StringIO()
  outfile = open('results_%s_%s_%s' % (fname, opts.query_num, datetime.datetime.now()), 'w')

  output.close()
  outfile.close()

if __name__ == "__main__":
  main()
