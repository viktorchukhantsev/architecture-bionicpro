from fastapi import FastAPI, Depends, HTTPException, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
import jwt
import requests
from jwt.algorithms import RSAAlgorithm
from decouple import config
from sqlalchemy.orm import Session
from database import SessionLocal
import datetime
import json
import io
from models import Report


KEYCLOAK_CLIENT_ID = config("KEYCLOAK_CLIENT_ID")
KEYCLOAK_URL = str(config("KEYCLOAK_URL"))
ISSUER_URL = str(config("ISSUER_URL"))
JWKS_URL = f"{KEYCLOAK_URL}/protocol/openid-connect/certs"
ALGORITHMS = ["RS256"]

origins = [
    str(config("ORIGINS")),
]

app = FastAPI()
security = HTTPBearer()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_jwks_cache = None


def get_jwks():
    global _jwks_cache
    if _jwks_cache is None:
        response = requests.get(JWKS_URL)
        response.raise_for_status()
        _jwks_cache = response.json()["keys"]
    return _jwks_cache


def get_public_key(token: str):
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(
            status_code=401, detail="Missing 'kid' in token header"
        )

    key = next((k for k in get_jwks() if k["kid"] == kid), None)
    if not key:
        raise HTTPException(
            status_code=401, detail="Public key not found"
        )

    return RSAAlgorithm.from_jwk(key)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_token(
        credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials
    try:
        public_key = get_public_key(token)
        payload = jwt.decode(
            token,
            public_key,
            algorithms=ALGORITHMS,
            # audience=KEYCLOAK_CLIENT_ID,
            # issuer=ISSUER_URL,
            options={"require": ["exp", "iat"]}
        )
        roles = payload.get("realm_access", {}).get("roles", [])
        if "prothetic_user" not in roles:
            raise HTTPException(
                status_code=401, detail="Insufficient role."
            )

        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=401, detail="Token expired."
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=401, detail=f"Invalid token: {str(e)}"
        )


@app.get("/reports")
def report(user=Depends(verify_token), db: Session = Depends(get_db)):
    user_name = user.get('preferred_username', 'unknown user')
    report_data = db.query(Report).filter(Report.owner == user_name)
    content = {
        "report_id": report_data,
        "requested_by": user_name
    }
    json_content = json.dumps(content, indent=2)
    today = datetime.date.today().strftime("%Y-%m-%d")
    file_name = f"report-{user_name}-{today}.json"
    string_io = io.StringIO(json_content)
    headers = {'Content-Disposition': f'attachment; filename="{file_name}"'}
    return StreamingResponse(
        content=iter([string_io.read()]),
        media_type="application/json",
        headers=headers
    )
