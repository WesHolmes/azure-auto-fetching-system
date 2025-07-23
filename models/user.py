from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class User:
    userPrincipalName: str
    accountEnabled: bool
    lastSignInDate: Optional[datetime] = None
    is_inactive: bool = False
    tenant_id: Optional[str] = None
    tenant_name: Optional[str] = None
    active_in_cw: bool = False
    id: Optional[str] = None

    def get_email(self) -> str:
        """
        Get the email address from the UPN, removing any tenant suffix
        """
        if "##" in self.userPrincipalName:
            return self.userPrincipalName.split("##")[0].lower()
        return self.userPrincipalName.lower()

    def __str__(self) -> str:
        """String representation for better logging"""
        tenant_info = (
            f" (Tenant: {self.tenant_name or self.tenant_id})" if self.tenant_id else ""
        )
        return f"User(upn={self.get_email()}, enabled={self.accountEnabled}, inactive={self.is_inactive}{tenant_info})"