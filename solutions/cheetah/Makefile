setup:
	RUSTFLAGS="-C target-cpu=native" cargo build --release

preproc:
	./target/release/cheetah preproc

compute:
	./target/release/cheetah compute

cleanup:
	rm -f data.bin
