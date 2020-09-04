import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine

def extract_csv():
    """
    Extract data, return dataframe from csv file
    """
    with open('export.csv') as export_file:
        return pd.read_csv(export_file)

def main():
    """
    Main function for ETL
    """
    with open('envmt.json') as db_config:
        db = json.load(db_config)
        try:
            # Establish a connection with the environment configuration specified in the envmt file
            db_conn = create_engine(f'oracle+cx_oracle://{db["user"]}:{db["pwd"]["csdev"]}@{db["host"]["csdev"]}:{db["port"]["csdev"]}/?service_name={db["svc"]["csdev"]}')
            print(f'Successfully created db connection on {db["svc"]["csdev"]}')

            # Grab the data from the survey export file and do a little modification to the format
            dataset = extract_csv()
            dataset.fillna(0, inplace=True)
            dataset['ID_4'] = pd.to_numeric(dataset['ID_4'], downcast='integer')
            dataset['Q3'] = pd.to_numeric(dataset['Q3'], downcast='integer')
            dataset['Q7'] = pd.to_numeric(dataset['Q7'], downcast='integer')

            # Keep a running list of jobs that the query can't find based on the given job_number
            failed_jobs = []

            # Iterate through the rows of the csv file 
            for job in dataset.itertuples():
                # See if the job_number exists in the table
                exists = db_conn.execute(f"""   
                                            SELECT 
                                                COUNT(1)
                                            FROM 
                                                workstudy_job 
                                            WHERE 
                                                job_number = '{job.ID_4}'
                                        """).fetchone()[0]
                if exists == 1:
                    # See if the job intends to allow remote
                    if job.Q2 == 'Yes':
                        print(f'Executing remote position update for job_number = {job.ID_4}')
                        # Update job information with the results of the survey
                        db_conn.execute(f"""
                                            UPDATE 
                                                workstudy_job
                                            SET 
                                                allow_remote = 'Y'
                                                ,current_remote_position = '{job.Q3}'
                                                ,additional_remote_position = '{job.Q7}'
                                            WHERE
                                                job_number = '{job.ID_4}'
                                        """)
                    else:
                        print(f'Opted out of remote positions for job_number = {job.ID_4}')
                else:
                    print(f'ERROR: Could not find job with job_number = {job.ID_4}')
                    failed_jobs.append(job.ID_4)

            # Generate a report file once the csv has been processed
            with open('reportfile.txt', 'w') as report:
                if len(failed_jobs) > 0:
                    report.write('The following job numbers were not found in the workstudy_jobs table:\n')
                    for job in failed_jobs:
                        report.write(f'{job}\n')
                    report.write('Please check the export file for possible errors in the specified job number.')
                else:
                    report.write('All jobs listed in the export have successfully been updated.')
                
        except Exception as e:
            print(f'Failed: {e}')


if __name__ == "__main__":

    try:
        print("Import process - Begin")

        # call the main function
        main()

        print("Import process - End")
    except Exception as error:
        print(f'Encountered an exception: {error}')
        sys.exit(1)