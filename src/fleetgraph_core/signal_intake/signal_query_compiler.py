"""Deterministic signal query string compilation."""

from copy import deepcopy


def validate_query_inputs(keywords=None, suffixes=None) -> None:
    """Validate optional keyword and suffix collections."""
    if keywords is not None:
        if not isinstance(keywords, list):
            raise TypeError("keywords must be a list or None")

        for keyword in keywords:
            if not isinstance(keyword, str):
                raise TypeError("each keyword must be a non-empty string")
            if keyword.strip() == "":
                raise ValueError("each keyword must be a non-empty string")

    if suffixes is not None:
        if not isinstance(suffixes, list):
            raise TypeError("suffixes must be a list or None")

        for suffix in suffixes:
            if not isinstance(suffix, str):
                raise TypeError("each suffix must be a non-empty string")
            if suffix.strip() == "":
                raise ValueError("each suffix must be a non-empty string")


def build_query_identity(query_text: str) -> tuple:
    """Build a deterministic query identity tuple."""
    if not isinstance(query_text, str):
        raise TypeError("query_text must be a string")
    if query_text == "":
        raise ValueError("query_text must be a non-empty string")
    return (query_text,)


def compile_signal_queries(keywords=None, suffixes=None) -> list[str]:
    """Compile deterministic query strings from keywords and suffixes."""
    validate_query_inputs(keywords=keywords, suffixes=suffixes)

    keyword_values = []
    suffix_values = []

    if keywords is not None:
        keyword_values = deepcopy(keywords)
    if suffixes is not None:
        suffix_values = deepcopy(suffixes)

    normalized_keywords = sorted({value.strip() for value in keyword_values})
    normalized_suffixes = sorted({value.strip() for value in suffix_values})

    if normalized_keywords and normalized_suffixes:
        query_texts = []
        for keyword in normalized_keywords:
            for suffix in normalized_suffixes:
                query_text = f"{keyword} {suffix}".strip()
                query_texts.append(query_text)
        return sorted(query_texts, key=build_query_identity)

    if normalized_keywords:
        return sorted(normalized_keywords, key=build_query_identity)

    if normalized_suffixes:
        return sorted(normalized_suffixes, key=build_query_identity)

    return []
