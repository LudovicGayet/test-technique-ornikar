SHELL := /bin/ksh

# RUNNING THE SCRIPT
run_script_1_b:
	@echo "Run du script Question 1.b"
	cd python_virtual_environment; poetry install; $$(poetry env info --path)/bin/python ../question_b.py -p EI -p EIRL -d 2020-02-01 -f 2020-09-01

# TESTING THE SCRIPT
test_script_1_b:
	@echo "Test du script Question 1.b"
	cd python_virtual_environment; poetry install; $$(poetry env info --path)/bin/pytest ../test_question_b.py