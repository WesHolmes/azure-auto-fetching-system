from dataclasses import dataclass
from typing import Optional


@dataclass
class Contact:
    id: str
    email: str
    inactive: bool = False
    update_reason: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    needs_ms_info_update: bool = False
    security_identifier: Optional[str] = None
    user_defined_field1: Optional[str] = None

    def __str__(self) -> str:
        """String representation for better logging"""
        name = (
            f"{self.first_name} {self.last_name}".strip()
            if self.first_name or self.last_name
            else "Unknown"
        )
        company = f" ({self.company_name})" if self.company_name else ""
        return f"Contact(id={self.id}, name={name}, email={self.email}, inactive={self.inactive}{company})"