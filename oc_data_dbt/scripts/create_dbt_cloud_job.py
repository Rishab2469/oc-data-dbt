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

# Raw Layer
create_job(
    "Raw Layer Build",
    [
        "dbt run --select path:models/raw/"
    ]
)
# Staging Layer
create_job(
    "Staging Layer Build",
    [
        "dbt run --select path:models/staging/"
    ]
)
# RDV Layer
create_job(
    "RDV Layer Build",
    [
        "dbt run --select path:models/rdv/"
    ]
)
# Analytics/Conform Layer
create_job(
    "Analytics Layer Build",
    [
        "dbt run --select path:models/analytics/"
    ]
)
