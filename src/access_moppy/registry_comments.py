"""Conservative, offline supplementation of optional CMIP7 variable comments.

CMOR's CMIP7 tables deliberately leave comments blank until they are
homogenised. The registry can supply a comment, but its alternatives are not
ordered preferences and may belong to different frequencies or regions.
"""

from __future__ import annotations

import warnings

# Exact problematic prose, not a permanent ban on these variables: a corrected
# registry description can be used without changing this list. See #823.
_COMMENT_CONFLICTS = {
    "fnup_tavg-u-hxy-lnd": {
        "The uptake of nitrogen by fixation: nitrogen fixation means the uptake "
        "of nitrogen gas directly from the atmosphere.": "fNup is plant nitrogen uptake; biological fixation is fBNF",
    },
    "siflfwbot_tavg-u-hxy-si": {
        "Total flux of fresh water from water into sea ice. This flux is positive "
        "when fresh water enters the ocean, and is therefore negative during "
        "ice growth and positive during ice melt.": "the stated transfer direction contradicts the ocean-positive sign convention",
    },
}


def get_cmip7_variable_comment(compound_name: str) -> str | None:
    """Return a unique applicable comment from the exact configured CV record.

    This performs no vocabulary installation or update. Missing databases,
    older schemas, missing compound identities and ambiguous alternatives are
    nonfatal: optional prose must not determine whether data can be produced.
    In particular, the root variable's description is never a fallback for a
    branded variable. ``selected_term_fields`` works with ESGVOC 6.1 and 7.2
    without requiring either version's complete record model.
    """
    parts = compound_name.split(".")
    if len(parts) != 5:
        return None
    branded_id = f"{parts[1]}_{parts[2]}".lower()

    def skip(reason: str) -> None:
        warnings.warn(
            f"CMIP7 optional comment for {compound_name} was not added: {reason}.",
            UserWarning,
            stacklevel=3,
        )

    try:
        from esgvoc.api import get_term_in_collection

        term = get_term_in_collection(
            project_id="cmip7",
            collection_id="branded_variable",
            term_id=branded_id,
            selected_term_fields=["id", "comment", "compound_name"],
        )
        record = term if isinstance(term, dict) else term.model_dump() if term else {}
    except Exception as exc:
        skip(f"local registry unavailable ({type(exc).__name__})")
        return None

    if str(record.get("id", "")).lower() != branded_id:
        skip("no exact branded-variable record")
        return None
    compounds = record.get("compound_name", [])
    if isinstance(compounds, str):
        compounds = [compounds]
    if not isinstance(compounds, (list, tuple)) or compound_name.lower() not in {
        name.lower() for name in compounds if isinstance(name, str)
    }:
        skip("registry does not establish this full compound identity")
        return None

    comments = record.get("comment", [])
    if isinstance(comments, str):
        comments = [comments]
    if not isinstance(comments, (list, tuple)) or any(
        not isinstance(comment, str) for comment in comments
    ):
        skip("registry has no usable comment description")
        return None
    comments = {comment.strip() for comment in comments if comment.strip()}
    if len(comments) != 1:
        skip(
            "registry has multiple permitted descriptions without an exact choice"
            if comments
            else "registry has no comment description"
        )
        return None
    comment = comments.pop()
    conflict = _COMMENT_CONFLICTS.get(branded_id, {}).get(comment)
    if (
        branded_id == "tossq_tavg-u-hxy-sea"
        and parts[3].lower() != "day"
        and comment == "Square of temperature of liquid ocean, averaged over the day."
    ):
        conflict = "the description specifies a daily average for a non-daily product"
    if conflict:
        skip(f"known vocabulary conflict: {conflict}")
        return None
    return comment
