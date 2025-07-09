import requests

# Replace these with your actual values
dbt_cloud_api_token = 'YOUR_DBT_CLOUD_API_TOKEN'
account_id = 'YOUR_ACCOUNT_ID'
project_id = 'YOUR_PROJECT_ID'

headers = {
    'Authorization': f'Token {dbt_cloud_api_token}',
    'Content-Type': 'application/json'
}


def create_job(job_name, steps):
    payload = {
        "name": job_name,
        "project_id": project_id,
        "execute_steps": steps,
        "triggers": {
            "github_webhook": False,
            "git_provider_webhook": False,
            "schedule": False
        }
    }
    response = requests.post(
        f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/",
        headers=headers,
        json=payload
    )
    print(f"Job: {job_name}")
    print(response.status_code)
    print(response.json())

# Raw layer job: create all external tables from config, then run raw models
create_job(
    "Raw Layer Build",
    [
        "dbt run-operation create_external_tables",
        "dbt run --select path:models/raw/"
    ]
)
# Staging and RDV jobs
create_job("Staging Layer Build", ["dbt run --select path:models/staging/"])
create_job("RDV Layer Build", ["dbt run --select path:models/rdv/"])
