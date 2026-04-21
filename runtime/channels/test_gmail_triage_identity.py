from runtime.channels.base import ChannelTarget
from runtime.channels.gmail_triage import _extract_merchant_identity


def test_sender_campaign_domain_does_not_become_merchant_candidate():
    target = ChannelTarget(
        channel="gmail",
        target_id="msg-1",
        thread_id="thread-1",
        title="Congrats on your FICO Score progress",
        body="Click here to review updates: https://click.e.usa.experian.com/review and manage your alerts.",
        author="news@e.usa.experian.com",
        metadata={
            "sender_email": "news@e.usa.experian.com",
            "sender_name": "Experian",
        },
    )
    result = _extract_merchant_identity(
        target,
        f"{target.title}\n{target.body}",
        "news@e.usa.experian.com",
        "e.usa.experian.com",
        False,
        "merchant_distress",
        [],
    )
    assert result == (None, None, None), result


def test_real_explicit_merchant_domain_still_wins():
    target = ChannelTarget(
        channel="gmail",
        target_id="msg-2",
        thread_id="thread-2",
        title="Shopify payouts paused",
        body="My store https://backupstore.myshopify.com is still on hold after review.",
        author="owner@merchantmail.com",
        metadata={
            "sender_email": "owner@merchantmail.com",
            "sender_name": "Backup Store",
        },
    )
    name, domain, confidence = _extract_merchant_identity(
        target,
        f"{target.title}\n{target.body}",
        "owner@merchantmail.com",
        "merchantmail.com",
        False,
        "merchant_distress",
        [],
    )
    assert domain == "backupstore.myshopify.com", (name, domain, confidence)
    assert confidence == 0.9, (name, domain, confidence)


if __name__ == "__main__":
    test_sender_campaign_domain_does_not_become_merchant_candidate()
    test_real_explicit_merchant_domain_still_wins()
    print("gmail triage identity tests passed")
