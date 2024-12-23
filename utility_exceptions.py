"""
Exceptions for utility
"""

class NotFound(Exception):
    """
    Raises when object not found
    """
    code = -404

class UtilityError(Exception):
    """
    Main exceptions utility
    """
    code = -1001
