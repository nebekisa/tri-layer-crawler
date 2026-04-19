"""
Authentication routes for the API.
"""

from fastapi import APIRouter, HTTPException, Depends
from src.api.auth import (
    AuthManager, 
    TokenResponse, 
    UserCredentials,
    require_auth,
    UserInfo
)

router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserCredentials):
    """
    Login and receive access/refresh tokens.
    
    Default credentials:
        - admin / admin123 (full access)
        - viewer / viewer123 (read-only)
    """
    token_response = AuthManager.login(credentials)
    
    if not token_response:
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password"
        )
    
    return token_response


@router.post("/refresh")
async def refresh(refresh_token: str):
    """Get new access token using refresh token."""
    new_token = AuthManager.refresh_access_token(refresh_token)
    
    if not new_token:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired refresh token"
        )
    
    return {
        "access_token": new_token,
        "token_type": "bearer",
        "expires_in": 3600
    }


@router.get("/me", response_model=UserInfo)
async def get_current_user_info(user: dict = Depends(require_auth)):
    """Get current authenticated user information."""
    return UserInfo(
        username=user["username"],
        role=user["role"],
        permissions=user["permissions"]
    )
