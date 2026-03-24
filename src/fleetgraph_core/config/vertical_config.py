"""Deterministic configuration for product vertical resolution."""

from typing import Any, Mapping


# Canonical supported verticals
_SUPPORTED_VERTICALS = (
    "fleet",
    "construction_audit_litigation",
)


def get_supported_verticals() -> tuple[str, ...]:
    """Return immutable tuple of supported vertical names.
    
    Returns:
        tuple[str, ...]: Canonical supported vertical identifiers.
    """
    return _SUPPORTED_VERTICALS


def is_supported_vertical(vertical: Any) -> bool:
    """Check if a vertical is supported without raising errors.
    
    Args:
        vertical: Value to check.
    
    Returns:
        bool: True only for exact canonical supported values, False otherwise.
    """
    if not isinstance(vertical, str):
        return False
    normalized = vertical.strip()
    return normalized in _SUPPORTED_VERTICALS


def validate_vertical(vertical: Any) -> str:
    """Validate and normalize a vertical identifier.
    
    Args:
        vertical: Value to validate.
    
    Returns:
        str: Canonical validated vertical string.
    
    Raises:
        ValueError: If vertical is not a string, empty, whitespace-only,
                   or not in the supported set.
    """
    if not isinstance(vertical, str):
        raise ValueError("vertical must be a string")
    
    normalized = vertical.strip()
    
    if not normalized:
        raise ValueError("vertical cannot be empty or whitespace-only")
    
    if normalized not in _SUPPORTED_VERTICALS:
        raise ValueError(f"vertical '{normalized}' is not supported")
    
    return normalized


def get_active_vertical(runtime_config: Mapping[str, Any] | None = None) -> str:
    """Resolve the active vertical deterministically.
    
    Resolution order:
    1. If runtime_config provided and contains 'vertical' field, use it.
    2. Otherwise default to 'fleet'.
    
    The resolved vertical is validated before being returned.
    
    Args:
        runtime_config: Optional mapping containing vertical configuration.
    
    Returns:
        str: Canonical active vertical string.
    
    Raises:
        ValueError: If runtime_config is not a mapping when supplied,
                   or if the vertical field contains an invalid value.
    """
    resolved_vertical = "fleet"
    
    if runtime_config is not None:
        if not isinstance(runtime_config, Mapping):
            raise ValueError("runtime_config must be a mapping")
        
        if "vertical" in runtime_config:
            resolved_vertical = runtime_config["vertical"]
    
    return validate_vertical(resolved_vertical)
