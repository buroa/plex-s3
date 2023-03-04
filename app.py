import os
import httpx
import boto3
import logging

from databases import Database
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

app = FastAPI()
client = httpx.AsyncClient()

plex_db = os.getenv("PLEX_DB")
plex_url = os.getenv("PLEX_URL")

bucket = os.getenv("AWS_S3_BUCKET")
region = os.getenv("AWS_S3_REGION")
endpoint = os.getenv("AWS_S3_ENDPOINT")
access_key_id = os.getenv("AWS_S3_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_S3_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    region_name=region,
    endpoint_url=endpoint,
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
)

database = Database(f"sqlite+aiosqlite:///{plex_db}?mode=ro")

query = """
    select file from media_parts where id={id} limit 0, 1
"""


@app.on_event("startup")
async def database_connect():
    await database.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()


def create_presigned_url(object_name, expiration=18000):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_name},
            ExpiresIn=expiration,
        )
    except Exception as e:
        logging.exception(e)
        return None
    else:
        return response


async def get_file_for_part(part):
    try:
        records = await database.fetch_one(query.format(id=part))
    except Exception as e:
        logging.exception(e)
    else:
        return records[0] if records else None


@app.get("/")
async def ping():
    return "Ping successful"


@app.api_route("/library/parts/{part}/{epoch}/{file}", methods=["GET", "PUT", "PATCH"])
async def stream(request: Request, part: str, epoch: int, file: str):
    if request.method == "GET":
        file = await get_file_for_part(part)

        if file:
            url = create_presigned_url(file[1:])

            if url:
                return RedirectResponse(url, status_code=307)

    url = httpx.URL(plex_url).join(request.url.path)
    req = client.build_request(
        request.method,
        url,
        headers=request.headers,
        params=request.query_params,
    )
    r = await client.send(req, stream=True)

    excluded_headers = [
        "content-encoding",
        "content-length",
        "transfer-encoding",
        "connection",
        "date",
    ]
    headers = {k: v for k, v in r.headers.items() if k.lower() not in excluded_headers}

    return StreamingResponse(
        r.aiter_text(),
        headers=headers,
        status_code=r.status_code,
        background=BackgroundTask(r.aclose),
    )
