import logging
import os
import time

import requests


logger = logging.getLogger(__name__)


class HIBPClient:
    def __init__(self):
        # Load both API keys
        self.api_keys = [
            os.getenv("HIBP_API_KEY_1") or os.getenv("HIBP_API_KEY"),
            os.getenv("HIBP_API_KEY_2"),
        ]
        # Filter out None values
        self.api_keys = [key for key in self.api_keys if key]

        if not self.api_keys:
            logger.error("No HIBP API keys configured")

        self.base_url = "https://haveibeenpwned.com/api/v3"
        self.user_agent = "HIBP-Sync-Azure-Function"
        self.max_retries = 3
        self.base_retry_delay = 1

        # Track current key and rate limit status
        self.current_key_index = 0
        self.key_rate_limited_until = {}  # key -> timestamp when available

    def _get_next_available_key(self) -> str | None:
        """Get the next available API key that's not rate limited."""
        current_time = time.time()

        # Clean up expired rate limits
        self.key_rate_limited_until = {key: until for key, until in self.key_rate_limited_until.items() if until > current_time}

        # Try to find a non-rate-limited key
        for _ in range(len(self.api_keys)):
            key = self.api_keys[self.current_key_index]
            if key not in self.key_rate_limited_until:
                return key

            # Move to next key
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)

        # All keys rate limited - return the one that will be available soonest
        if self.key_rate_limited_until:
            soonest_key = min(self.key_rate_limited_until.items(), key=lambda x: x[1])[0]
            wait_time = self.key_rate_limited_until[soonest_key] - current_time
            if wait_time > 0:
                logger.info(f"All keys rate limited, waiting {wait_time:.1f}s")
                time.sleep(wait_time + 1)
            del self.key_rate_limited_until[soonest_key]
            return soonest_key

        return self.api_keys[0] if self.api_keys else None

    def check_email_breaches(self, email: str) -> list[dict] | None:
        """Check if an email has been in any breaches."""
        if not self.api_keys:
            logger.error("No HIBP API keys configured")
            return None

        url = f"{self.base_url}/breachedaccount/{email}"
        params = {"truncateResponse": "false"}

        for attempt in range(self.max_retries):
            api_key = self._get_next_available_key()
            if not api_key:
                logger.error("No API keys available")
                return None

            headers = {"hibp-api-key": api_key, "user-agent": self.user_agent}

            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    # No breaches found - this is a good thing
                    return []
                elif response.status_code == 429:
                    # Rate limited - mark this key and try another
                    retry_after = int(response.headers.get("retry-after", 60))
                    self.key_rate_limited_until[api_key] = time.time() + retry_after
                    logger.info(f"Rate limited on key {self.current_key_index + 1}, switching keys")

                    # Move to next key for next attempt
                    self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
                    continue
                elif response.status_code in [400, 401, 403]:
                    logger.error(f"Client error {response.status_code} for {email}")
                    return None
                else:
                    logger.warning(f"Unexpected status {response.status_code} for {email}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.base_retry_delay * (2**attempt))
                        continue
                    return None

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout checking {email} (attempt {attempt + 1})")
                if attempt < self.max_retries - 1:
                    time.sleep(self.base_retry_delay * (2**attempt))
                    continue
                return None
            except Exception as e:
                logger.error(f"Error checking {email}: {str(e)}")
                return None

        return None
