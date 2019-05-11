import re
def make_input_cached(query):
  return query.replace("uservisits", "uservisits_cached") \
              .replace("rankings", "rankings_cached") \
              .replace("url_counts_partial", "url_counts_partial_cached") \
              .replace("url_counts_total", "url_counts_total_cached") \
              .replace("documents", "documents_cached")

def make_output_cached(query):
  return query.replace(TMP_TABLE, TMP_TABLE_CACHED)
