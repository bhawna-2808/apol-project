import requests
from bs4 import BeautifulSoup
from typing import List, Tuple
from uuid import uuid4
from celery import celery

def scrape_page(url: str) -> List[Tuple[str, str]]:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    for row in soup.select("tr[data-cy=ResultRow]"):
        name = row.select_one("a[data-cy=ResultName]").text.strip()