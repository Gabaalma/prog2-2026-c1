setup:
	pip install scikit-learn pandas flask

preproc:
	rm -f out.csv
	ln -s /dev/shm/out.csv out.csv
	rm -f query.csv
	ln -s /dev/shm/query.csv query.csv
	rm -f trigger.pipe done.pipe
	mkfifo trigger.pipe done.pipe
	python fifo.py &
	cat done.pipe

compute:
	echo go > trigger.pipe

cleanup:
	echo nothing
