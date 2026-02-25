import azure.functions as func
import json
import os
from azure.cosmos import CosmosClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

ENDPOINT = os.environ["COSMOS_ENDPOINT"]
KEY = os.environ["COSMOS_KEY"]
DATABASE = "salesIntel"
CONTAINER = "maingraph"

client = CosmosClient(ENDPOINT, credential=KEY)
container = client.get_database_client(DATABASE).get_container_client(CONTAINER)

@app.route(route="query_cosmos")
def query_cosmos(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        query = body.get("query", "SELECT TOP 10 * FROM c")

        items = list(container.query_items(query, enable_cross_partition_query=True))

        return func.HttpResponse(
            json.dumps({"results": items}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )