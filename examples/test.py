import json
import os
import time

from dotenv import load_dotenv
import requests

load_dotenv()

dummy_data = [
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405825-1",
        "DocumentCode": "0405825",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Credit Memo",
        "PostingDate": "2019-01-08T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-1781",
        "ProductID": "bin-PMMA-A67",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": -40,
        "NetSalesEUR": -686,
        "GrossProfitEUR": -156.72,
        "CalcGrossProfitEUR": -156.72,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405826-1",
        "DocumentCode": "0405826",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-01-08T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-8343",
        "ProductID": "bin-POM-FM090",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 1125,
        "NetSalesEUR": 2362.5,
        "GrossProfitEUR": 420.02,
        "CalcGrossProfitEUR": 420.02,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405827-1",
        "DocumentCode": "0405827",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-01-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-7299",
        "ProductID": "bin-TPE-VL12040DNC",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 5000,
        "NetSalesEUR": 14250,
        "GrossProfitEUR": 2020,
        "CalcGrossProfitEUR": 2020,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405828-1",
        "DocumentCode": "0405828",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-01-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-9686",
        "ProductID": "bin-PA6-6B30300",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 2000,
        "NetSalesEUR": 5800,
        "GrossProfitEUR": 208.16,
        "CalcGrossProfitEUR": 208.16,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405828-2",
        "DocumentCode": "0405828",
        "DocumentLineNr": 2,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-01-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-9686",
        "ProductID": "bin-PA6-6G30300",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 2000,
        "NetSalesEUR": 5600,
        "GrossProfitEUR": 292.93,
        "CalcGrossProfitEUR": 292.93,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405829-1",
        "DocumentCode": "0405829",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-03-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-9686",
        "ProductID": "bin-PA6-6B30300",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 2000,
        "NetSalesEUR": 5800,
        "GrossProfitEUR": 208.16,
        "CalcGrossProfitEUR": 208.16,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405829-2",
        "DocumentCode": "0405829",
        "DocumentLineNr": 2,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-04-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-9686",
        "ProductID": "bin-PA6-6G30300",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "EUR",
        "TransactionExchangeRate": 1,
        "Quantity": 1850,
        "NetSalesEUR": 5180,
        "GrossProfitEUR": 217.95,
        "CalcGrossProfitEUR": 217.95,
    },
    {
        "CompanyCode": "bin",
        "DataSourceCode": "ASC",
        "DocumentID": "bin-0405830-1",
        "DocumentCode": "0405830",
        "DocumentLineNr": 1,
        "SourceType": "Stock",
        "SalesType": "Sales",
        "DocumentType": "Sales Invoice",
        "PostingDate": "2019-01-09T00:00:00Z",
        "LocationCode": "?",
        "CustomerID": "bin-1502",
        "ProductID": "bin-TPE-SD16140ANC",
        "TransactionSupplierID": "bin-2999",
        "SalesUnit": "kg",
        "CurrencyCode": "CHF",
        "TransactionExchangeRate": 0.876117,
        "Quantity": 1500,
        "NetSalesEUR": 3876.817725,
        "GrossProfitEUR": 933.660365,
        "CalcGrossProfitEUR": 933.660365,
    },
]


def get_status(status_uri: str):
    res = requests.get(status_uri)
    if res.ok:
        data = res.json()
        if data["runtimeStatus"] not in ("Pending", "Running"):
            print(f"Execution {data['runtimeStatus']}")
            with open("output.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        else:
            print(f"{data['runtimeStatus']}...")
            time.sleep(1)
            return get_status(status_uri)
    else:
        raise requests.HTTPError(response=res)


DEV_PATH = "http://localhost:7071/api/orchestrators/execute_notebook?notebook_path=test/test.ipynb"
PROD_PATH = "https://jupyter-notebook-as-a-function.azurewebsites.net/api/orchestrators/execute_notebook?notebook_path=test/test.ipynb"

res = requests.post(
    DEV_PATH,
    json={"data": {"sales_transactions": dummy_data}},
    headers={"x-functions-key": os.environ["FUNCTION_KEY"]},
    timeout=30,
)
if res.ok:
    data = res.json()
    get_status(status_uri=data["statusQueryGetUri"])
else:
    raise requests.HTTPError(response=res)
