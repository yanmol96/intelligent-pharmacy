from fastapi import FastAPI, Request
import httpx

app = FastAPI(title="API Gateway")

SERVICES = {
    "inventory": "http://inventory-service:8000",
    "orders": "http://order-prescription-service:8000",
    "pharmacy": "http://pharmacy-service:8000",
    "ml": "http://ml-service:8000",
}

async def proxy_request(service_url: str, request: Request, path: str):
    async with httpx.AsyncClient() as client:
        url = f"{service_url}/{path}"
        if request.url.query:
            url += f"?{request.url.query}"


        response = await client.request(
            method=request.method,
            url=url,
            headers=request.headers.raw,
            content=await request.body()
        )

        return response.content


@app.api_route("/inventory/{path:path}", methods=["GET","POST","PUT","DELETE"])
async def inventory_proxy(path: str, request: Request):
    return await proxy_request(SERVICES["inventory"], request, path)


@app.api_route("/orders/{path:path}", methods=["GET","POST","PUT","DELETE"])
async def orders_proxy(path: str, request: Request):
    return await proxy_request(SERVICES["orders"], request, path)


@app.api_route("/pharmacy/{path:path}", methods=["GET","POST","PUT","DELETE"])
async def pharmacy_proxy(path: str, request: Request):
    return await proxy_request(SERVICES["pharmacy"], request, path)


@app.api_route("/ml/{path:path}", methods=["GET","POST","PUT","DELETE"])
async def ml_proxy(path: str, request: Request):
    return await proxy_request(SERVICES["ml"], request, path)


@app.get("/")
def root():
    return {"gateway": "running"}
