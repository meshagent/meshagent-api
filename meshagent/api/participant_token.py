import os
import jwt
from typing import Optional, List
from datetime import datetime

class ParticipantGrant:
    def __init__(self, *, name: str, scope: Optional[str]):
        self.name = name
        self.scope = scope

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "scope": self.scope
        }

    @staticmethod
    def from_json(data: dict) -> 'ParticipantGrant':
        return ParticipantGrant(
            name=data["name"],
            scope=data["scope"]
        )

class ParticipantToken:
    def __init__(self, *, name: str, project_id: Optional[str] = None, api_key_id: str = None, grants: Optional[List[ParticipantGrant]] = None, extra_payload: Optional[dict] = None):
        if grants is None:
            grants = []
        self.name = name
        self.grants = grants
        self.project_id = project_id
        self.api_key_id = api_key_id
        self.extra_payload = extra_payload

    @property
    def role(self):
        for grant in self.grants:
            if grant.name == "role" and grant.scope != "user":
                return grant.scope
            
        return "user"

    @property
    def is_user(self):
        for grant in self.grants:
            if grant.name == "role" and grant.scope != "user":
                return False
            
        return True

    def add_role_grant(self, role: str):
        self.grants.append(ParticipantGrant(name="role", scope=role))

    def add_room_grant(self, room_name: str):
        self.grants.append(ParticipantGrant(name="room", scope=room_name))

    def grant_scope(self, name: str) -> str | None:
        for g in self.grants:
            if g.name == name:
                return g.scope
            
        return None

    def to_json(self) -> dict:
        j = {
            "name": self.name,
            "grants": [g.to_json() for g in self.grants]
        }

        if self.project_id != None:
            j["sub"] = self.project_id

        if self.api_key_id != None:
            j["kid"] = self.api_key_id

        return j
    
    def to_jwt(self, *, token: Optional[str] = None, expiration: Optional[datetime] = None) -> str:
        

        extra_payload = self.extra_payload
        if extra_payload is None:
            extra_payload = {}
        else:
            extra_payload = extra_payload.copy()

        if expiration != None:
            extra_payload["exp"] = expiration

        payload = self.to_json()
        
        if token == None:
             token = os.getenv("MESHAGENT_SECRET")
             if "kid" in payload:
                # We are exporting a token using the default secret, so we should remove the key id
                payload.pop("kid")
             
        return jwt.encode(payload=payload, key=token, algorithm='HS256')

    @staticmethod
    def from_json(data: dict) -> 'ParticipantToken':
        data = data.copy()
        name = data.pop("name")
        grants = data.pop("grants")
        project_id = None
        api_key_id = None

        if "sub" in data:
            project_id = data.pop("sub")

        if "kid" in data:
            api_key_id = data.pop("kid")


        return ParticipantToken(
            name=name,
            project_id=project_id,
            api_key_id=api_key_id,
            grants=[ParticipantGrant.from_json(g) for g in grants],
            extra_payload=data
        )

    @staticmethod
    def from_jwt(jwt_str: str, *, token: Optional[str] = None) -> 'ParticipantToken':
        if token == None:
             token = os.getenv("MESHAGENT_SECRET")

        decoded = jwt.decode(jwt=jwt_str, key=token, algorithms=['HS256'])
        return ParticipantToken.from_json(decoded)
    