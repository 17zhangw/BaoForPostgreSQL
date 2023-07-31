#!/bin/bash

set -ex

#cd ~/mythril/
#python3 scripts/replay_job_full.py --bao-file /home/wz2/Bao/bao_run1.txt &> bao_out1.txt
#cd ~/Bao

B_ARMS=( 5 5 5 5 5 5 49 49 49 49 49 49 )
B_DURATIONS=( 8 8 8 8 8 8 8 8 8 8 8 8 )
B_PQT=( 300 300 300 30 30 30 300 300 300 30 30 30 )

idx=0

for ((i = 0; i < ${#B_ARMS[@]}; i++));
do
	ARMS="${B_ARMS[i]}"
	DURATION="${B_DURATIONS[i]}"
	PQT="${B_PQT[i]}"

	OUT_LOG=bao_out$idx.log

	cd bao_server
	python3 main.py &
	cd ..

	python3 run_queries.py \
		--duration $DURATION \
		--port 5433 \
		--num-arms $ARMS \
		--per-query-timeout $PQT | tee $OUT_LOG

	sleep 5
	kill -9 $(ps aux | grep '[p]ython3 main.py' | awk '{print $2}')
	sleep 5
	rm -rf ~/Bao/bao_server/bao_previous_model
	rm -rf ~/Bao/bao_server/bao_default_model
	rm -rf ~/Bao/bao_server/bao.db

	idx=$((idx+1))
done

#python3 run_queries.py | tee bao_run2.txt
