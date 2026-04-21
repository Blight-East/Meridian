from runtime.ingestion.sources.shopify_community_signals import (
    _parse_board_threads,
    _parse_thread_detail,
    _thread_fingerprint,
    _thread_should_refresh,
)
from runtime.ingestion.sources.stack_overflow_signals import (
    _parse_question_detail,
    _parse_tag_listing,
)
from runtime.intelligence.public_web_source_utils import (
    classify_source_candidate,
    format_signal_content,
    select_best_merchant_domain,
    should_emit_signal,
)


SHOPIFY_BOARD_HTML = """
<table class='topic-list'>
  <tbody>
    <tr class="topic-list-item">
      <td class="main-link">
        <span class="link-top-line">
          <a class="title" href="https://community.shopify.com/t/urgent-shopify-payments-payouts-paused-with-no-reason-over-nzd-3800-on-hold/591084">
            [URGENT] Shopify Payments payouts paused with NO REASON, over NZD 3800 on hold
          </a>
        </span>
      </td>
      <td class="replies"><span class="posts">2</span></td>
    </tr>
  </tbody>
</table>
"""

SHOPIFY_THREAD_HTML = """
<div class="topic-body crawler-post" id="post_1">
  <div class="crawler-post-meta">
    <span class="creator"><span itemprop="name">Kolor_Me</span></span>
    <span class="crawler-post-infos"><time class="post-time" datetime="2026-03-05T20:27:24Z"></time></span>
  </div>
  <div class="post" itemprop="text">
    <p>My store https://northstarcart.co is still selling but Shopify Payments payouts are paused and the account is under review.</p>
  </div>
</div>
<div class="topic-body crawler-post" id="post_2">
  <div class="crawler-post-meta">
    <span class="creator"><span itemprop="name">OtherMerchant</span></span>
    <span class="crawler-post-infos"><time class="post-time" datetime="2026-03-05T22:27:24Z"></time></span>
  </div>
  <div class="post" itemprop="text">
    <p>Same issue on my shop https://backupstore.myshopify.com and payouts are still on hold.</p>
  </div>
</div>
"""

STACK_TAG_HTML = """
<div class="s-post-summary">
  <div class="s-post-summary--content">
    <h3><a class="s-link" href="/questions/12345/shopify-payment-provider-is-not-enabled-on-checkout">Shopify payment provider is not enabled on checkout</a></h3>
    <div class="s-post-summary--content-excerpt">Our merchant checkout returns payment provider is not enabled and webhooks are failing.</div>
  </div>
  <div class="s-post-summary--stats">
    <div class="s-post-summary--stats-item">1 answer</div>
  </div>
</div>
"""

STACK_QUESTION_HTML = """
<div class="question js-question" data-author-username="devmerchant">
  <div class="post-layout">
    <div class="js-post-body">
      <p>Our store https://merchant-payments.co fails checkout with 'payment provider is not enabled' after switching gateways.</p>
      <p>We also see webhook retries in the payment flow.</p>
    </div>
  </div>
  <time itemprop="dateCreated" datetime="2026-03-10T15:10:00Z"></time>
</div>
<div class="answer">
  <div class="js-post-body">
    <p>This usually happens when the provider is disabled in production mode.</p>
  </div>
</div>
<ul class="comments-list">
  <li class="comment">Merchant confirmed this only happens on the live store.</li>
</ul>
"""


def test_domain_selection():
    domain, ranked = select_best_merchant_domain(
        "My store is https://northstarcart.co and payouts are paused.",
        ["https://northstarcart.co/contact"],
    )
    assert domain == "northstarcart.co", (domain, ranked)


def test_shopify_community_parsing():
    threads = _parse_board_threads("Payments + Shipping & Fulfilment", SHOPIFY_BOARD_HTML)
    assert len(threads) == 1, threads
    parsed = _parse_thread_detail(threads[0], SHOPIFY_THREAD_HTML)
    assert len(parsed) == 2, parsed
    parent = parsed[0]
    assert parent["merchant_domain"] == "northstarcart.co", parent
    assert parent["distress_type"] == "payouts_delayed", parent
    assert parent["processor"] == "shopify_payments", parent
    ok, reason = should_emit_signal(parent, allow_without_domain=True)
    assert ok, reason
    reply = parsed[1]
    assert reply["merchant_domain"] == "backupstore.myshopify.com", reply


def test_stack_overflow_parsing():
    listing = _parse_tag_listing("shopify", STACK_TAG_HTML)
    assert len(listing) == 1, listing
    question = _parse_question_detail(listing[0], STACK_QUESTION_HTML)
    assert question, question
    assert question["merchant_domain"] == "merchant-payments.co", question
    assert question["distress_type"] == "onboarding_rejected", question
    ok, reason = should_emit_signal(question, allow_without_domain=False)
    assert ok, reason


def test_source_candidate_classification():
    classified = classify_source_candidate(
        title="Shopify Payments disabled after review",
        body_text="My store can still take orders but Shopify Payments is disabled and payouts are paused.",
        merchant_domain="northstarcart.co",
    )
    assert classified["processor"] == "shopify_payments", classified
    assert classified["distress_type"] in {"account_frozen", "payouts_delayed"}, classified
    assert classified["confidence"] >= 0.6, classified


def test_thread_refresh_fingerprint_changes():
    fingerprint_a = _thread_fingerprint("Thread A", 2, "March 5, 2026")
    fingerprint_b = _thread_fingerprint("Thread A", 3, "March 5, 2026")
    assert fingerprint_a != fingerprint_b
    should_refresh, reason, _ = _thread_should_refresh(
        {
            "thread_or_question_url": "https://community.shopify.com/t/example/1",
            "thread_fingerprint": fingerprint_a,
            "reply_count": 2,
            "last_activity_label": "March 5, 2026",
        }
    )
    assert should_refresh, reason


def test_domainless_signal_content_marks_attribution_state():
    content = format_signal_content(
        {
            "source_label": "Shopify Community",
            "source_category": "Payments + Shipping & Fulfilment",
            "processor": "shopify_payments",
            "distress_type": "payouts_delayed",
            "confidence": 0.8,
            "title": "Payouts paused",
            "body_text": "My store payouts are paused.",
            "thread_or_question_url": "https://community.shopify.com/t/example/1",
            "merchant_attribution_state": "domainless",
        }
    )
    assert "Merchant attribution state: domainless." in content, content


if __name__ == "__main__":
    test_domain_selection()
    test_shopify_community_parsing()
    test_stack_overflow_parsing()
    test_source_candidate_classification()
    test_thread_refresh_fingerprint_changes()
    test_domainless_signal_content_marks_attribution_state()
    print("public web source tests passed")
