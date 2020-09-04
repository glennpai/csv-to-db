# csv-to-db
Tool used to update a database table based on the results of a survey

Author: Christopher Glenn

Contact: glennc@ohio.edu

## SETUP
1. Place survey results file in project root folder
    
    * Name file 'export.csv'
    * Ensure format matches that of 'export-template.csv'

2. Create 'envmt.json' file in project root folder

    * Change values for each environment

3. Point the script at the appropriate environment in the main function

    * This can be done by searching the 'db-script.py' for ```dev``` by default and replacing any instances with ```tst``` for Test, ```qat``` for QA, or ```prd``` for Production. 

## RUNNING THE SCRIPT
To run the script, execute ```python3 db-scipt.py```

The script will check each row in the CSV file, search the DB for a matching job number, modify the entry for the job when appropriate, and generate a report file. 

The report file generated summarizes the status of the job after runtime. If the script was unable to locate jobs by their job number, they will be listed. If all jobs were found, this will be noted in the file. 