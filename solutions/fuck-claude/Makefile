setup:
	pip install scikit-learn pandas flask

preproc:
	rm -f out.csv query.csv trigger.pipe done.pipe
	ln -s /dev/shm/out.csv out.csv
	ln -s /dev/shm/query.csv query.csv
	mkfifo trigger.pipe done.pipe
	python fifo.py &
	cat done.pipe

compute:
	echo go > trigger.pipe
	cat done.pipe

cleanup:
	echo nothing
