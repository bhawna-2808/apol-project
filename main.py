from fastapi import FastAPI, HTTPException
from typing import List, Tuple
from uuid import uuid4
from celery import Celery
import redis
import requests

app = FastAPI()
celery = Celery("tasks", broker="pyamqp://guest@localhost//")
redis_client = redis.Redis(host="localhost", port=6379, db=0)

def scrape_page(url: str) -> List[Tuple[str, str]]:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    for row in soup.select("tr[data-cy=ResultRow]"):
        name = row.select_one("a[data-cy=ResultName]").text.strip()
        organization_name = row.select_one("div[data-cy=ResultCompany] a").text.strip()
        results.append((name, organization_name))
    return results

@celery.task
def scrape_jobs(name: str, organization_name: str, job_id: str) -> None:
    base_url = "https://www.apollo.io/search/people"
    params = {
        "first_name": name,
        "last_name": "",
        "company": organization_name,
        "location": "",
        "page": 1,
    }
    results = []
    for page in range(1, 6):
        params["page"] = page
        url = f"{base_url}?{requests.utils.urlencode(params)}"
        results.extend(scrape_page(url))
    redis_client.set(job_id, results)

@app.post("/scrape")
def start_scrape(name: str, organization_name: str) -> dict:
    job_id = str(uuid4())
    scrape_jobs.delay(name, organization_name, job_id)
    return {"job_id": job_id}

@app.get("/scrape_results/{job_id}")
def get_scrape_results(job_id: str) -> List[Tuple[str, str]]:
    results = redis_client.get(job_id)
    if results is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return pickle.loads(results)