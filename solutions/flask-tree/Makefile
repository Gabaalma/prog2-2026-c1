setup:
	pip install scikit-learn pandas flask

preproc:
	screen -dmS flask_server python3 preproc.py; while ! nc -z localhost 5678; do sleep 1; done;


compute:
	curl http://127.0.0.1:5678/ping

cleanup:
	pkill -f flask_server
