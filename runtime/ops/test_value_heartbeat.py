from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OPERATOR_REVIEW_ONLY,
    ELIGIBILITY_OUTREACH,
)
from runtime.ops.value_heartbeat import _meets_signal_candidate_scout_bar
from runtime.intelligence.distress_normalization import normalize_distress_topic


def test_outreach_candidate_keeps_direct_outreach_bar():
    accepted, reason, _ = _meets_signal_candidate_scout_bar(
        eligibility=ELIGIBILITY_OUTREACH,
        icp_fit_score=60,
        quality_score=60,
    )
    assert accepted is True, (accepted, reason)

    accepted, reason, why = _meets_signal_candidate_scout_bar(
        eligibility=ELIGIBILITY_OUTREACH,
        icp_fit_score=55,
        quality_score=72,
    )
    assert accepted is False, (accepted, reason, why)
    assert reason == "icp_fit_too_low", (accepted, reason, why)


def test_operator_review_candidate_gets_review_bar():
    accepted, reason, _ = _meets_signal_candidate_scout_bar(
        eligibility=ELIGIBILITY_OPERATOR_REVIEW_ONLY,
        icp_fit_score=45,
        quality_score=40,
    )
    assert accepted is True, (accepted, reason)

    accepted, reason, why = _meets_signal_candidate_scout_bar(
        eligibility=ELIGIBILITY_OPERATOR_REVIEW_ONLY,
        icp_fit_score=42,
        quality_score=58,
    )
    assert accepted is False, (accepted, reason, why)
    assert reason == "icp_fit_too_low", (accepted, reason, why)


def test_gateway_cutoff_phrase_normalizes_to_real_distress():
    distress = normalize_distress_topic(
        "Our payment gateway recently stopped accepting tobacco related product sales."
    )
    assert distress != "unknown", distress


if __name__ == "__main__":
    test_outreach_candidate_keeps_direct_outreach_bar()
    test_operator_review_candidate_gets_review_bar()
    test_gateway_cutoff_phrase_normalizes_to_real_distress()
    print("value heartbeat tests passed")
