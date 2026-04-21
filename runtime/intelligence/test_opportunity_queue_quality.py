from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_CONSUMER_NOISE,
    ELIGIBILITY_DOMAIN_UNPROMOTABLE,
    ELIGIBILITY_IDENTITY_WEAK,
    ELIGIBILITY_OPERATOR_REVIEW_ONLY,
    ELIGIBILITY_OUTREACH,
    ELIGIBILITY_RESEARCH_ONLY,
    evaluate_opportunity_queue_quality,
    summarize_quality_counts,
)


def test_outreach_eligible_merchant_distress():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "northstarcart.co",
            "merchant_name": "Northstar Cart",
            "processor": "stripe",
            "distress_topic": "account_frozen",
        },
        merchant={
            "canonical_name": "Northstar Cart",
            "domain": "northstarcart.co",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "[gmail][processor:stripe][distress:account_frozen] Subject: Northstar Cart payouts escalation Merchant candidate: Northstar Cart",
        },
    )
    assert quality["eligibility_class"] == ELIGIBILITY_OUTREACH, quality


def test_consumer_noise_detection():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "retailer.com",
            "merchant_name": "Retailer",
            "processor": "paypal",
            "distress_topic": "consumer complaints",
        },
        merchant={
            "canonical_name": "Retailer",
            "domain": "retailer.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "My order never arrived and customer service will not give me my money back.",
        },
    )
    assert quality["eligibility_class"] == ELIGIBILITY_CONSUMER_NOISE, quality


def test_domain_unpromotable_detection():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "reuters.com",
            "merchant_name": "Reuters",
            "processor": "unknown",
            "distress_topic": "unclassified merchant distress",
        },
        merchant={
            "canonical_name": "Reuters",
            "domain": "reuters.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Reuters article about shipping pressure and regional trade disruption.",
        },
    )
    assert quality["eligibility_class"] == ELIGIBILITY_DOMAIN_UNPROMOTABLE, quality


def test_publisher_subdomain_is_unpromotable():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "legal.thomsonreuters.com",
            "merchant_name": "Thomson Reuters Legal",
            "processor": "unknown",
            "distress_topic": "account_frozen",
        },
        merchant={
            "canonical_name": "Thomson Reuters Legal",
            "domain": "legal.thomsonreuters.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Legal article about merchant verification reviews and compliance trends.",
        },
    )
    assert quality["eligibility_class"] == ELIGIBILITY_DOMAIN_UNPROMOTABLE, quality


def test_identity_weak_detection():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "",
            "merchant_name": "Unknown Stripe Merchant",
            "processor": "stripe",
            "distress_topic": "chargeback_issue",
        },
        merchant={
            "canonical_name": "Unknown Stripe Merchant",
            "domain": "",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Stripe chargeback issue.",
        },
    )
    assert quality["eligibility_class"] == ELIGIBILITY_IDENTITY_WEAK, quality


def test_research_and_operator_review_classes():
    research = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "northstarshop.com",
            "merchant_name": "Northstar Notes",
            "processor": "stripe",
            "distress_topic": "unknown",
        },
        merchant={
            "canonical_name": "Northstar Notes",
            "domain": "northstarshop.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "General discussion about processor alternatives and commerce tooling trends.",
        },
    )
    assert research["eligibility_class"] == ELIGIBILITY_RESEARCH_ONLY, research

    review = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "northstarhq.co",
            "merchant_name": "Northstar HQ",
            "processor": "stripe",
            "distress_topic": "unclassified merchant distress",
        },
        merchant={
            "canonical_name": "Northstar HQ",
            "domain": "northstarhq.co",
            "domain_confidence": "provisional",
        },
        signal={
            "content": "Merchant candidate: Northstar HQ. Stripe payout review is ongoing and operations are impacted.",
        },
    )
    assert review["eligibility_class"] == ELIGIBILITY_OPERATOR_REVIEW_ONLY, review
    assert review["icp_fit_score"] >= 45, review


def test_hosted_storefront_stays_reviewable():
    review = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "backupstore.myshopify.com",
            "merchant_name": "Backup Store",
            "processor": "shopify_payments",
            "distress_topic": "payouts_delayed",
        },
        merchant={
            "canonical_name": "Backup Store",
            "domain": "backupstore.myshopify.com",
            "domain_confidence": "provisional",
            "status": "candidate",
        },
        signal={
            "content": "Merchant candidate: Backup Store. My shop https://backupstore.myshopify.com is still on hold and payouts are delayed after a Shopify Payments review.",
        },
    )
    assert review["eligibility_class"] == ELIGIBILITY_OPERATOR_REVIEW_ONLY, review
    assert review["domain_quality_label"] != "unpromotable", review
    assert review["icp_fit_score"] >= 45, review


def test_gateway_cutoff_distress_is_not_left_unknown():
    review = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "inaustralia.myshopify.com",
            "merchant_name": "In Australia",
            "processor": "unknown",
            "distress_topic": "unknown",
        },
        merchant={
            "canonical_name": "In Australia",
            "domain": "inaustralia.myshopify.com",
            "domain_confidence": "provisional",
            "status": "candidate",
        },
        signal={
            "content": "Our payment gateway recently stopped accepting tobacco related product sales after a year and we need a new solution to accept payments online via card.",
        },
    )
    assert review["parsed_distress_type"] != "unknown", review
    assert review["distress_quality_score"] >= 35, review


def test_contact_discovered_with_real_distress_gets_canonical_parse():
    review = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "exactly.com",
            "merchant_name": "Exactly",
            "processor": "paypal",
            "distress_topic": "contact_discovered",
        },
        merchant={
            "canonical_name": "Exactly",
            "domain": "exactly.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Exactly. Same boat here, fully legal normal business. I have had bank accounts frozen and PayPal reverting my business account to personal.",
        },
    )
    assert review["parsed_distress_type"] == "account_frozen", review
    assert review["eligibility_class"] in {ELIGIBILITY_OUTREACH, ELIGIBILITY_OPERATOR_REVIEW_ONLY}, review


def test_contact_discovered_without_clear_distress_stays_suppressed():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "whats.com",
            "merchant_name": "Whats",
            "processor": "unknown",
            "distress_topic": "contact_discovered",
        },
        merchant={
            "canonical_name": "Whats",
            "domain": "whats.com",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Ask HN: Whats the best Payment Gateway for a small Startup?",
        },
    )
    assert quality["parsed_distress_type"] == "unknown", quality
    assert quality["eligibility_class"] == ELIGIBILITY_RESEARCH_ONLY, quality


def test_historical_or_hypothetical_signal_stays_out_of_outreach():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "northstarcart.co",
            "merchant_name": "Northstar Cart",
            "processor": "stripe",
            "distress_topic": "chargeback_issue",
        },
        merchant={
            "canonical_name": "Northstar Cart",
            "domain": "northstarcart.co",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Case study: last year Northstar Cart had chargeback pressure after a promotion.",
        },
    )
    assert quality["eligibility_class"] != ELIGIBILITY_OUTREACH, quality
    assert quality["icp_fit_label"] == "weak", quality


def test_generic_tooling_case_is_not_high_conviction():
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_domain": "northstarcart.co",
            "merchant_name": "Northstar Cart",
            "processor": "stripe",
            "distress_topic": "chargeback_issue",
        },
        merchant={
            "canonical_name": "Northstar Cart",
            "domain": "northstarcart.co",
            "domain_confidence": "confirmed",
        },
        signal={
            "content": "Northstar Cart is shopping for a generic analytics dashboard and marketing automation tooling.",
        },
    )
    assert quality["eligibility_class"] != ELIGIBILITY_OUTREACH, quality
    assert quality["icp_fit_label"] == "weak", quality


def test_quality_summary_counts():
    counts = summarize_quality_counts(
        [
            {"eligibility_class": ELIGIBILITY_OUTREACH},
            {"eligibility_class": ELIGIBILITY_RESEARCH_ONLY},
            {"eligibility_class": ELIGIBILITY_DOMAIN_UNPROMOTABLE},
            {"eligibility_class": ELIGIBILITY_IDENTITY_WEAK},
        ]
    )
    assert counts["outreach_eligible_opportunities"] == 1, counts
    assert counts["suppressed_total"] == 3, counts


if __name__ == "__main__":
    test_outreach_eligible_merchant_distress()
    test_consumer_noise_detection()
    test_domain_unpromotable_detection()
    test_publisher_subdomain_is_unpromotable()
    test_identity_weak_detection()
    test_research_and_operator_review_classes()
    test_hosted_storefront_stays_reviewable()
    test_historical_or_hypothetical_signal_stays_out_of_outreach()
    test_generic_tooling_case_is_not_high_conviction()
    test_contact_discovered_with_real_distress_gets_canonical_parse()
    test_contact_discovered_without_clear_distress_stays_suppressed()
    test_quality_summary_counts()
    print("opportunity queue quality tests passed")
