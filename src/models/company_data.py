import json, os
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()
COMPANIES_JSON_PATH = os.getenv("COMPANIES_JSON_PATH")

@dataclass
class Company:
    name: str
    ticker: str

class CompanyDataModel:

    def __init__(self, json_path = COMPANIES_JSON_PATH):
        self.json_path = json_path
        self.companies: List[Company] = []
        self.load_companies()

    def load_companies(self) -> None:
        with open(self.json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.companies = [
                Company(name=item["name"], ticker=item["ticker"])
                for item in data
            ]

    def get_name_by_ticker(self, ticker: str) -> Optional[str]:
        for company in self.companies:
            if company.ticker.upper() == ticker.upper():
                return company.name
        return None

    def get_companies(self) -> list[Company]:
        return self.companies


