import sqlite3
import json
import itertools

from common import BaoException

def _bao_db(db):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("""
CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY,
    pg_pid INTEGER,
    plan TEXT, 
    reward REAL
)""")
    c.execute("""
CREATE TABLE IF NOT EXISTS experimental_query (
    id INTEGER PRIMARY KEY, 
    query TEXT UNIQUE
)""")
    c.execute("""
CREATE TABLE IF NOT EXISTS experience_for_experimental (
    experience_id INTEGER,
    experimental_id INTEGER,
    arm_idx INTEGER,
    FOREIGN KEY (experience_id) REFERENCES experience(id),
    FOREIGN KEY (experimental_id) REFERENCES experimental_query(id),
    PRIMARY KEY (experience_id, experimental_id, arm_idx)
)""")
    conn.commit()
    return conn

def record_reward(db, plan, reward, pid):
    with _bao_db(db) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO experience (plan, reward, pg_pid) VALUES (?, ?, ?)",
                  (json.dumps(plan), reward, pid))
        conn.commit()

    print("Logged reward of", reward)

def clear_experience(db):
    with _bao_db(db) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM experience")
        conn.commit()

def experience(db):
    with _bao_db(db) as conn:
        c = conn.cursor()
        c.execute("SELECT plan, reward FROM experience")
        return c.fetchall()

def experiment_experience(db):
    all_experiment_experience = []
    for res in experiment_results(db):
        all_experiment_experience.extend(
            [(x["plan"], x["reward"]) for x in res]
        )
    return all_experiment_experience
    
def experiment_results(db):
    with _bao_db(db) as conn:
        c = conn.cursor()
        c.execute("""
SELECT eq.id, e.reward, e.plan, efe.arm_idx
FROM experimental_query eq, 
     experience_for_experimental efe, 
     experience e 
WHERE eq.id = efe.experimental_id AND e.id = efe.experience_id
ORDER BY eq.id, efe.arm_idx;
""")
        for eq_id, grp in itertools.groupby(c, key=lambda x: x[0]):
            yield ({"reward": x[1], "plan": x[2], "arm": x[3]} for x in grp)
        

# select eq.id, efe.arm_idx, min(e.reward) from experimental_query eq, experience_for_experimental efe, experience e WHERE eq.id = efe.experimental_id AND e.id = efe.experience_id GROUP BY eq.id;
