setup:
	pip install pandas numpy pyarrow

preproc:
	python preproc.py

compute:
	python src.py

cleanup:
	-del /f global_movies.npz grid_data.npz grid_params.json