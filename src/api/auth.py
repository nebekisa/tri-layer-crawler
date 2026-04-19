"""
JWT Authentication for Tri-Layer Intelligence Crawler API.

Provides:
    - Token generation (login)
    - Token validation middleware
    - Role-based access control
    - Token refresh
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel
from passlib.context import CryptContext

logger = logging.getLogger(__name__)


# Configuration
SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key-change-in-production')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS = 7


# Models
class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class UserCredentials(BaseModel):
    username: str
    password: str


class UserInfo(BaseModel):
    username: str
    role: str
    permissions: list[str]


# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)


class AuthManager:
    """JWT Authentication Manager."""
    
    # In production, use a database
    _users = {
        "admin": {
            "username": "admin",
            "password_hash": pwd_context.hash("admin123"),
            "role": "admin",
            "permissions": ["read", "write", "delete", "admin"]
        },
        "viewer": {
            "username": "viewer",
            "password_hash": pwd_context.hash("viewer123"),
            "role": "viewer",
            "permissions": ["read"]
        }
    }
    
    @classmethod
    def create_access_token(cls, data: Dict[str, Any], 
                            expires_delta: Optional[timedelta] = None) -> str:
        """Create a new access token."""
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })
        
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    @classmethod
    def create_refresh_token(cls, data: Dict[str, Any]) -> str:
        """Create a refresh token."""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        })
        
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    @classmethod
    def decode_token(cls, token: str) -> Optional[Dict[str, Any]]:
        """Decode and validate a token."""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    @classmethod
    def authenticate_user(cls, username: str, password: str) -> Optional[Dict]:
        """Authenticate user credentials."""
        user = cls._users.get(username)
        if not user:
            return None
        
        if not pwd_context.verify(password, user["password_hash"]):
            return None
        
        return {
            "username": user["username"],
            "role": user["role"],
            "permissions": user["permissions"]
        }
    
    @classmethod
    def login(cls, credentials: UserCredentials) -> Optional[TokenResponse]:
        """Login and generate tokens."""
        user = cls.authenticate_user(credentials.username, credentials.password)
        if not user:
            return None
        
        token_data = {
            "sub": user["username"],
            "role": user["role"],
            "permissions": user["permissions"]
        }
        
        access_token = cls.create_access_token(token_data)
        refresh_token = cls.create_refresh_token({"sub": user["username"]})
        
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )
    
    @classmethod
    def refresh_access_token(cls, refresh_token: str) -> Optional[str]:
        """Generate new access token from refresh token."""
        payload = cls.decode_token(refresh_token)
        if not payload or payload.get("type") != "refresh":
            return None
        
        username = payload.get("sub")
        user = cls._users.get(username)
        if not user:
            return None
        
        token_data = {
            "sub": user["username"],
            "role": user["role"],
            "permissions": user["permissions"]
        }
        
        return cls.create_access_token(token_data)


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
) -> Optional[Dict[str, Any]]:
    """
    Dependency to get current authenticated user.
    
    Returns None if no token provided (allows public endpoints).
    Raises HTTPException if token is invalid.
    """
    if credentials is None:
        return None
    
    token = credentials.credentials
    payload = AuthManager.decode_token(token)
    
    if payload is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=401,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return {
        "username": payload.get("sub"),
        "role": payload.get("role"),
        "permissions": payload.get("permissions", [])
    }


async def require_auth(user: Optional[Dict] = Depends(get_current_user)):
    """Dependency to require authentication."""
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def require_role(required_role: str):
    """Dependency factory to require specific role."""
    async def role_checker(user: Dict = Depends(require_auth)):
        if user.get("role") != required_role and user.get("role") != "admin":
            raise HTTPException(
                status_code=403,
                detail=f"Role '{required_role}' required"
            )
        return user
    return role_checker


async def require_permission(permission: str):
    """Dependency factory to require specific permission."""
    async def permission_checker(user: Dict = Depends(require_auth)):
        permissions = user.get("permissions", [])
        if permission not in permissions and "admin" not in permissions:
            raise HTTPException(
                status_code=403,
                detail=f"Permission '{permission}' required"
            )
        return user
    return permission_checker
