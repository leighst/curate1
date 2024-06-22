lint:
	python3 -m venv .venv

	# Make the sandbox active in the current shell session
	. .venv/bin/activate

	# Install pinned pip first
	pip install -r $$(git rev-parse --show-toplevel)/py/pip-requirements.txt

	# Install shared development dependencies and project/library-specific dependencies
	pip install -r $$(git rev-parse --show-toplevel)/py/dev-requirements.txt -r requirements.txt

	# With project-specific dependencies installed, typecheck your code as follows:
	pyright .
