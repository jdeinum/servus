all:
	@echo "target should be clean or test"



clean:
	rm -r __pycache__/ MASTER.sqlite tables/


test:
	make clean;
	python3 create.py;
	sqlite3 MASTER.sqlite
