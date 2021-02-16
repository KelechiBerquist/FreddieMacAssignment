# Single Family Loan File Reader Application
This application: 
- takes in a zipped csv file; 
- reads the contents of the file in chunks without unzipping the file; 
- and loads the content to a db.


## Execution
Activate the python virtual environment using
```
source ./app/.venv/bin/activate
```
Execute the bash script that runs the python application using
``` 
bash app/execute.sh 
```
### Arguments to the python execution command 
- The first argument is the path to the database connection variables file
	- The default location is the parent directory of the application.
	- A sample is included file is included.
- The second argument is the path to the data file to be ingested
	- The default location is a folder named `data` in the parent directory of the application.


Log files are written to `./logs/` <br/>
Insertion and test report files are written to `./report/`