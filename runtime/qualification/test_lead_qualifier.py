from runtime.qualification.lead_qualifier import qualify_lead


def test_live_processor_distress_is_pipeline_eligible():
    result = qualify_lead(
        "Our store is under Stripe review right now, payouts are delayed, and chargebacks are escalating on six figures of monthly revenue."
    )
    assert result["eligible_for_pipeline"] is True, result
    assert result["icp_fit_label"] in {"strong", "medium"}, result


def test_historical_case_is_suppressed():
    result = qualify_lead(
        "Case study: last year our store had payout issues with Stripe after a promotion."
    )
    assert result["eligible_for_pipeline"] is False, result
    assert result["disqualifier_reason"] == "historical_or_hypothetical", result


def test_generic_tooling_case_is_suppressed():
    result = qualify_lead(
        "We are looking for generic analytics and marketing automation software for our ecommerce store."
    )
    assert result["eligible_for_pipeline"] is False, result
    assert result["disqualifier_reason"] == "generic_tooling_mismatch", result


if __name__ == "__main__":
    test_live_processor_distress_is_pipeline_eligible()
    test_historical_case_is_suppressed()
    test_generic_tooling_case_is_suppressed()
    print("lead qualifier tests passed")
