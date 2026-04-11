from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any

from fleetgraph.messaging.message_engine import generate_message_drafts, resolve_signal_family

__all__ = [
    "generate_optimized_message_drafts",
    "get_template_variant_registry",
    "select_message_variant",
]

_ALLOWED_SIGNAL_FAMILIES = (
    "litigation",
    "payment",
    "enforcement",
    "generic",
)
_ALLOWED_BUCKETS = (
    "T1",
    "T2",
    "T3A",
    "T3B",
    "T3C",
)
_BASE_PRODUCT_LINE = (
    "FleetGraph helps teams keep matter and project documentation organized so requests, "
    "reviews, and handoffs take less manual chasing."
)
_BASE_CTA_LINE = (
    "If this is relevant on your side, the easiest next step is usually to review the "
    "self-serve workflow your team could use to evaluate it directly."
)
_SUBJECT_STYLE_TEMPLATES = {
    "baseline": "{base_subject}",
    "documentation_focus": "Documentation workflow question for {company_name}",
}
_PRODUCT_STYLE_LINES = {
    "baseline": _BASE_PRODUCT_LINE,
    "concise": "FleetGraph is built to keep matter and project documentation organized so teams can respond with less manual follow-up.",
}
_CTA_STYLE_LINES = {
    "self_serve_standard": _BASE_CTA_LINE,
    "self_serve_guided": "If this is relevant on your side, the simplest next step is usually to review the self-serve workflow and see whether it fits how your team handles records today.",
}
_VARIANT_LIBRARY = {
    "v1": {
        "template_variant_id": "v1",
        "variant_name": "baseline",
        "copy_style": "balanced",
        "variant_group_suffix": "baseline",
        "subject_style": "baseline",
        "product_style": "baseline",
        "cta_style": "self_serve_standard",
    },
    "v2": {
        "template_variant_id": "v2",
        "variant_name": "concise",
        "copy_style": "concise",
        "variant_group_suffix": "concise",
        "subject_style": "documentation_focus",
        "product_style": "concise",
        "cta_style": "self_serve_guided",
    },
}
_VARIANT_REGISTRY = {
    "litigation": {
        "T1": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T2": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3A": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3B": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3C": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
    },
    "payment": {
        "T1": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T2": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3A": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3B": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3C": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
    },
    "enforcement": {
        "T1": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T2": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3A": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3B": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3C": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
    },
    "generic": {
        "T1": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T2": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3A": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3B": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
        "T3C": {"v1": deepcopy(_VARIANT_LIBRARY["v1"]), "v2": deepcopy(_VARIANT_LIBRARY["v2"])} ,
    },
}


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional string fields must be strings or None")
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _variant_override_key(*, prospect_id: str, contact_email: str) -> str:
    return f"{prospect_id}|{contact_email.lower()}"


def get_template_variant_registry() -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    return deepcopy(_VARIANT_REGISTRY)


def select_message_variant(
    *,
    signal_family: str,
    selected_bucket: str,
    prospect_id: str,
    contact_email: str,
    campaign_key: str | None = None,
    variant_overrides: dict[str, str] | None = None,
    campaign_variant_defaults: dict[str, str] | None = None,
) -> dict[str, str]:
    normalized_signal_family = _normalize_non_empty_string(signal_family, field_name="signal_family").lower()
    normalized_selected_bucket = _normalize_non_empty_string(selected_bucket, field_name="selected_bucket")
    if normalized_signal_family not in _ALLOWED_SIGNAL_FAMILIES:
        normalized_signal_family = "generic"
    if normalized_selected_bucket not in _ALLOWED_BUCKETS:
        raise ValueError("selected_bucket is invalid")

    registry_entry = _VARIANT_REGISTRY[normalized_signal_family][normalized_selected_bucket]
    normalized_variant_overrides = dict(variant_overrides or {})
    normalized_campaign_variant_defaults = dict(campaign_variant_defaults or {})
    override_key = _variant_override_key(prospect_id=prospect_id, contact_email=contact_email)
    if override_key in normalized_variant_overrides:
        selected_variant_id = _normalize_non_empty_string(
            normalized_variant_overrides[override_key],
            field_name="variant_overrides value",
        )
        selection_mode = "override"
    else:
        campaign_default_key = f"{normalized_signal_family}|{normalized_selected_bucket}"
        if campaign_key is not None and campaign_default_key in normalized_campaign_variant_defaults:
            selected_variant_id = _normalize_non_empty_string(
                normalized_campaign_variant_defaults[campaign_default_key],
                field_name="campaign_variant_defaults value",
            )
            selection_mode = "campaign_default"
        else:
            selected_variant_id = "v1"
            selection_mode = "default"

    if selected_variant_id not in registry_entry:
        raise ValueError("selected variant is not registered")

    selected_variant = deepcopy(registry_entry[selected_variant_id])
    selected_variant["selection_mode"] = selection_mode
    selected_variant["variant_group"] = f"{normalized_signal_family}_{normalized_selected_bucket}_{selected_variant['variant_group_suffix']}"
    return selected_variant


def _render_subject(*, base_subject: str, company_name: str, subject_style: str) -> str:
    if subject_style not in _SUBJECT_STYLE_TEMPLATES:
        raise ValueError("subject_style is invalid")
    return _SUBJECT_STYLE_TEMPLATES[subject_style].format(
        base_subject=base_subject,
        company_name=company_name,
    )


def _render_body(*, base_body: str, product_style: str, cta_style: str) -> str:
    if product_style not in _PRODUCT_STYLE_LINES:
        raise ValueError("product_style is invalid")
    if cta_style not in _CTA_STYLE_LINES:
        raise ValueError("cta_style is invalid")
    rendered_body = base_body.replace(_BASE_PRODUCT_LINE, _PRODUCT_STYLE_LINES[product_style])
    rendered_body = rendered_body.replace(_BASE_CTA_LINE, _CTA_STYLE_LINES[cta_style])
    if "{{" in rendered_body or "}}" in rendered_body:
        raise ValueError("unresolved placeholders remain in body")
    return rendered_body


def _message_optimization_id(values: dict[str, Any]) -> str:
    digest_source = json.dumps(values, ensure_ascii=True, separators=(",", ":"))
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return f"message-opt:{digest}"


def generate_optimized_message_drafts(
    prospects: list[object],
    *,
    campaign_key: str | None = None,
    variant_overrides: dict[str, str] | None = None,
    campaign_variant_defaults: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    baseline_drafts = generate_message_drafts(prospects)
    optimized_drafts: list[dict[str, Any]] = []
    normalized_campaign_key = _normalize_optional_string(campaign_key)

    for baseline_draft in baseline_drafts:
        template_family = resolve_signal_family(
            signal_type=str(baseline_draft["signal_type"]),
            signal_detail=str(baseline_draft["signal_detail"]),
        )
        selected_variant = select_message_variant(
            signal_family=template_family,
            selected_bucket=str(baseline_draft["selected_bucket"]),
            prospect_id=str(baseline_draft["prospect_id"]),
            contact_email=str(baseline_draft["contact"]["email"]),
            campaign_key=normalized_campaign_key,
            variant_overrides=variant_overrides,
            campaign_variant_defaults=campaign_variant_defaults,
        )
        rendered_subject = _render_subject(
            base_subject=str(baseline_draft["subject"]),
            company_name=str(baseline_draft["company_name"]),
            subject_style=str(selected_variant["subject_style"]),
        )
        rendered_body = _render_body(
            base_body=str(baseline_draft["body"]),
            product_style=str(selected_variant["product_style"]),
            cta_style=str(selected_variant["cta_style"]),
        )
        if "{{" in rendered_subject or "}}" in rendered_subject:
            raise ValueError("unresolved placeholders remain in subject")
        optimized_draft = deepcopy(baseline_draft)
        optimized_draft["subject"] = rendered_subject
        optimized_draft["body"] = rendered_body
        optimized_draft["template_family"] = template_family
        optimized_draft["template_variant_id"] = str(selected_variant["template_variant_id"])
        optimized_draft["message_optimization_id"] = _message_optimization_id(
            {
                "prospect_id": optimized_draft["prospect_id"],
                "contact_email": optimized_draft["contact"]["email"],
                "template_family": optimized_draft["template_family"],
                "template_variant_id": optimized_draft["template_variant_id"],
                "campaign_key": normalized_campaign_key,
            }
        )
        optimized_draft["optimization_metadata"] = {
            "variant_group": str(selected_variant["variant_group"]),
            "selection_mode": str(selected_variant["selection_mode"]),
            "copy_style": str(selected_variant["copy_style"]),
        }
        optimized_drafts.append(optimized_draft)

    return optimized_drafts
