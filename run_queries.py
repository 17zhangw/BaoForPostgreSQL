import psycopg
import os
import sys
import random
from time import time, sleep
from pathlib import Path
from psycopg.errors import QueryCanceled
import argparse

parser = argparse.ArgumentParser(prog="Bao")
parser.add_argument("--duration", type=int, required=True)
parser.add_argument("--port", type=str, required=True)
parser.add_argument("--num-arms", type=int, required=True)
parser.add_argument("--per-query-timeout", type=int, required=True)
args = parser.parse_args()


USE_BAO = True
PG_CONNECTION_STR = f"dbname=benchbase user=admin host=localhost port={args.port}"
QUERY_ORDER = "/home/wz2/mythril/queries/job_full/qorder.txt"
DURATION_SEC = args.duration * 3600

# https://stackoverflow.com/questions/312443/
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def explain_queries(queries):
    conn = psycopg.connect(PG_CONNECTION_STR)
    cur = conn.cursor()
    cur.execute(f"SET enable_bao TO ON")
    cur.execute(f"SET enable_bao_selection TO OFF")
    cur.execute(f"SET enable_bao_rewards TO OFF")
    cur.execute(f"SET bao_num_arms TO {args.num_arms}")
    cur.execute(f"SET statement_timeout TO {args.per_query_timeout * 1000}")

    results = {}
    for (fp, q) in queries:
        r = [r for r in cur.execute("EXPLAIN " + q)]
        for (result_line,) in r:
            if "Bao recommended hint: " in result_line:
                results[fp] = result_line.split("Bao recommended hint: ")[-1]
    conn.close()
    return results


def run_query(sql, bao_select=False, bao_reward=False):
    start = time()
    while True:
        try:
            conn = psycopg.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute(f"SET bao_num_arms TO {args.num_arms}")
            cur.execute(f"SET statement_timeout TO {args.per_query_timeout * 1000}")
            cur.execute(q)
            cur.fetchall()
            conn.close()
            break
        except QueryCanceled:
            break
        except:
            sleep(1)
            continue
    stop = time()
    return stop - start

queries = []
with open(QUERY_ORDER, "r") as f:
    parent = Path(QUERY_ORDER).parent
    for line in f:
        sql = line.strip()
        with open(parent / sql, "r") as s:
            queries.append((str(parent / sql), s.read()))

print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

print("Executing queries using PG optimizer for initial training")
for q_idx, (fp, q) in enumerate(queries):
    pg_time = run_query(q, bao_reward=True)
    print("x", q_idx, time(), fp, pg_time, "PG", flush=True)


start_time = 0
cur_time = 0
next_time = start_time + 3600 * 0.25
c_idx = 0
while cur_time - start_time < DURATION_SEC:
    delta_start = time()
    if USE_BAO:
        os.system("cd bao_server && python3 baoctl.py --retrain")
        os.system("sync")

    for q_idx, (fp, q) in enumerate(queries):
        q_time = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO)
        print(c_idx, q_idx, time(), fp, q_time, flush=True)

    # Only track this time.
    cur_time += (time() - delta_start)

    if cur_time - start_time >= next_time:
        print("CHECKING CONFIG", flush=True)
        print(explain_queries(queries), flush=True)
        next_time += 3600 * 0.25

    c_idx += 1

print("CHECKING CONFIG", flush=True)
print(explain_queries(queries), flush=True)
