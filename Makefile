.PHONY: test demo compile clean

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

demo:
	PYTHONPATH=src python3 scripts/run_demo.py

compile:
	PYTHONPATH=src python3 scripts/compile_platform.py --spec-dir pipelines --output artifacts/generated

clean:
	find artifacts/generated -type f -delete 2>/dev/null || true

