setup:
	pip install scikit-learn pandas flask

preproc:
	./comm.sh

compute:
	curl http://127.0.0.1:5678/ping

cleanup:
	pkill -f flask_server
