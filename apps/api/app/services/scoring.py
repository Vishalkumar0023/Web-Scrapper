def bounded(value: float) -> float:
    return max(0.0, min(1.0, value))


def field_score(
    selector_stability: float,
    label_proximity: float,
    format_validity: float,
    template_reliability: float,
    signal_agreement: float,
) -> float:
    # Weighted scoring defined in scoring-engine-spec.md
    score = (
        0.35 * selector_stability
        + 0.20 * label_proximity
        + 0.20 * format_validity
        + 0.15 * template_reliability
        + 0.10 * signal_agreement
    )
    return round(bounded(score), 2)
