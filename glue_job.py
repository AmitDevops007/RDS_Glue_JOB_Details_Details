import boto3
import pymysql
import os

# AWS Clients
glue_client = boto3.client("glue", region_name="us-east-1")

# MySQL RDS Connection Details (Ensure you replace with actual values)
RDS_HOST = "rds-cft-mydbinstance-nch8sjt2bb8m.cdewcuym6u3h.us-east-1.rds.amazonaws.com"
RDS_USER = "Mysqldb"
RDS_PASSWORD = "admin1234"
RDS_DB = "MYSQLDB"

# Connect to MySQL RDS
def get_db_connection():
    return pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB,
        cursorclass=pymysql.cursors.DictCursor
    )

# Fetch Glue Jobs Details
def fetch_glue_jobs():
    response = glue_client.get_jobs()
    jobs = response.get("Jobs", [])

    conn = get_db_connection()
    with conn.cursor() as cursor:
        for job in jobs:
            sql = """
                INSERT INTO glue_jobs (job_name, role, created_on, last_modified_on, command_script, glue_version)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                role=VALUES(role), last_modified_on=VALUES(last_modified_on), command_script=VALUES(command_script);
            """
            cursor.execute(sql, (
                job["Name"],
                job["Role"],
                job["CreatedOn"],
                job["LastModifiedOn"],
                job["Command"]["ScriptLocation"],
                job["GlueVersion"]
            ))
    conn.commit()
    conn.close()

# Fetch Glue Job Runs
def fetch_glue_job_runs():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        # Fetch existing job names
        cursor.execute("SELECT job_name FROM glue_jobs")
        jobs = cursor.fetchall()

        for job in jobs:
            job_name = job["job_name"]
            response = glue_client.get_job_runs(JobName=job_name)
            for run in response["JobRuns"]:
                sql = """
                    INSERT INTO glue_job_runs (run_id, job_name, started_on, completed_on, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    status=VALUES(status), completed_on=VALUES(completed_on), error_message=VALUES(error_message);
                """
                cursor.execute(sql, (
                    run["Id"],
                    job_name,
                    run.get("StartedOn"),
                    run.get("CompletedOn"),
                    run["JobRunState"],
                    run.get("ErrorMessage", "")
                ))
    conn.commit()
    conn.close()

# Run Both Functions
fetch_glue_jobs()
fetch_glue_job_runs()
