setup:
	pip install scikit-learn pandas polars flask

# preproc:
# 	rm -f out.csv
# 	ln -s /dev/shm/out.csv out.csv
# 	rm -f query.csv
# 	ln -s /dev/shm/query.csv query.csv
# 	screen -dmS flask_server gunicorn -w 1 -k sync preproc:app -b 127.0.0.1:5678 --pid gunicorn.pid; while ! nc -z localhost 5678; do sleep 1; done;

# compute:
	# curl http://127.0.0.1:5678/ping

preproc:
	rm -f trigger.pipe done.pipe
	mkfifo trigger.pipe done.pipe
	python fifo.py &
	cat done.pipe

compute:
	echo go > trigger.pipe

cleanup:
	# kill `cat gunicorn.pid`
	# rm gunicorn.pid
	echo nothing
