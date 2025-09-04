#!/usr/bin/env python3
"""
Environment configuration helper for loading local.settings.json
"""

import json
import logging
import os


logger = logging.getLogger(__name__)


def load_local_settings():
    """Load environment variables from local.settings.json if it exists"""
    settings_file = os.path.join(os.path.dirname(__file__), "..", "local.settings.json")

    if os.path.exists(settings_file):
        try:
            with open(settings_file) as f:
                settings = json.load(f)
                values = settings.get("Values", {})

                # Set environment variables
                for key, value in values.items():
                    os.environ[key] = str(value)

                logger.info(f"✅ Loaded {len(values)} settings from local.settings.json")
                return True

        except Exception as e:
            logger.error(f"❌ Error loading local.settings.json: {e}")
            return False
    else:
        logger.info("ℹ️  local.settings.json not found")
        return False


# Auto-load when module is imported
load_local_settings()
