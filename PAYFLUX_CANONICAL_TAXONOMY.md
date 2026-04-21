# PayFlux Canonical Taxonomy

Last reconstructed: March 13, 2026

This document is the canonical internal ontology for PayFlux as implemented today across the live Agent Flux / PayFlux production system.

It is reconstructed from three sources only:
- existing repository code and tests
- live production data on `159.65.45.64`
- existing operator-facing language already used by the system

It does not invent new categories. Where the system has drift, that drift is called out explicitly.

## 1. What PayFlux Is

PayFlux is a merchant-distress intelligence and operator-assist system focused on businesses whose payment processing is blocked, degraded, under review, or becoming unreliable.

In the live system, PayFlux does five things:
- detects signals that look like merchant payment distress
- resolves those signals to merchants when possible
- creates operator-reviewed opportunities
- assembles merchant and pattern intelligence for decision support
- drafts operator-approved outreach for qualifying Gmail threads

The internal mental model is:

`signal -> merchant -> opportunity -> operator-reviewed action`

The system is optimized for:
- merchant operators, not end-consumer complaints
- processor-linked distress, not generic support
- operator review and recommendation, not unbounded autonomy

## 2. What PayFlux Is Not

PayFlux is not:
- a consumer-support desk
- a generic inbox triager for every email
- a general CRM for all sales activity
- an autonomous sender with no approval gates
- a catch-all taxonomy for every fintech or commerce concept

This is grounded in live code:
- consumer complaints are filtered or down-ranked in `merchant_signal_classifier.py` and `entity_taxonomy.py`
- Gmail sends remain `approval_required`
- `noise_system` and `test_thread` rows are explicitly suppressed from signal and opportunity creation

## 3. Merchant Taxonomy

### 3.1 Canonical merchant object

The live merchant record is centered on:
- `merchants.id`
- `canonical_name`
- `domain`
- `status`
- `industry`
- `distress_score`
- `signal_count`

Observed live merchant statuses:
- `active`
- `provisional`

Interpretation:
- `active`: a merchant identity the system considers materially resolved enough to use in intelligence and opportunities
- `provisional`: a tentative merchant identity, often created during propagation or extraction before identity quality is fully proven

### 3.2 Merchant identity quality

The live system treats merchant identity as layered:
- strong identity: business domain plus plausible merchant name
- medium identity: merchant name or domain only
- weak identity: provisional extracted token with no stable domain

Observed production drift:
- good resolved merchant: `Northstarcart` / `northstarcart.co`
- weak provisional examples: `AM PST`, `Risk Team`, `Payouts`, `Desperate Plea`

Canonical rule:
- operator-facing display may normalize names for readability
- stored canonical merchant records are not mutated by the display layer

### 3.3 Industry taxonomy

The live canonical industry enum used by Gmail triage and reasoning is:
- `ecommerce`
- `saas`
- `agency`
- `creator`
- `marketplace`
- `health_wellness`
- `high_risk`
- `unknown`

Observed live production values:
- `ecommerce`
- `unknown`

## 4. Processor Taxonomy

### 4.1 Canonical current processor enum

The live PayFlux processor enum is:
- `stripe`
- `paypal`
- `square`
- `adyen`
- `shopify_payments`
- `braintree`
- `authorize_net`
- `worldpay`
- `checkout_com`
- `unknown`

This enum is used consistently in:
- Gmail triage
- reasoning schemas
- PayFlux intelligence patterns
- merchant profile assembly

Observed live production processor values:
- Gmail thread intelligence: `stripe`, `unknown`
- distress patterns: `stripe`, `paypal`, `shopify_payments`, `unknown`

### 4.2 Historical / non-canonical processor references

Older or broader code references also mention:
- `mollie`
- `2checkout`
- `payoneer`
- `klarna`
- `afterpay`
- `wise`
- `revolut`
- `paddle`
- `rakuten pay`

These are not part of the current canonical PayFlux processor enum used by the live Gmail + reasoning + pattern pipeline.

Canonical rule:
- treat the 9-value enum above plus `unknown` as authoritative for live operator reasoning
- treat other processor mentions as historical, adjacent, or source-specific references until normalized

## 5. Distress Taxonomy

### 5.1 Canonical primary distress taxonomy

The current canonical primary distress enum is:
- `account_frozen`
- `payouts_delayed`
- `reserve_hold`
- `verification_review`
- `processor_switch_intent`
- `chargeback_issue`
- `account_terminated`
- `onboarding_rejected`
- `unknown`

This comes directly from:
- Gmail triage
- reasoning schemas
- PayFlux pattern intelligence

Observed live production values:
- Gmail thread intelligence currently shows only `NULL` in live rows because the inbox is mostly noise/system/test mail right now
- distress patterns show:
  - `account_frozen`
  - `account_terminated`
  - `payouts_delayed`
  - `reserve_hold`
  - `unknown`

### 5.2 Canonical secondary distress signals

For `merchant_distress` threads, the canonical secondary distress taxonomy is:
- `frozen_funds`
- `payout_paused`
- `reserve_increase`
- `compliance_review`
- `website_verification`
- `kyc_kyb_review`
- `rolling_reserve`
- `processing_disabled`
- `looking_for_alternative`
- `negative_balance`
- `dispute_spike`
- `unknown`

### 5.3 Historical / free-text distress drift

The live `merchant_opportunities.distress_topic` field is not yet canonical. Observed values include:
- `Unclassified merchant distress`
- `Consumer Complaints`
- `funds held`
- `testing distress gate`

Canonical rule:
- for operator reasoning, use canonical distress enums when available
- treat `merchant_opportunities.distress_topic` as partially normalized legacy text
- prefer linked signal context, Gmail intelligence, and pattern intelligence over raw opportunity distress text when they disagree

## 6. Signal Classification Taxonomy

### 6.1 Canonical signal classes in the live `signals` table

Observed production values:
- `merchant_distress`
- `consumer_complaint`
- `NULL`

Canonical interpretation:
- `merchant_distress`: a signal that reflects business/operator pain tied to payments, onboarding, reserves, payout delays, processor switching, or similar merchant-operational distress
- `consumer_complaint`: an end-user complaint, low-value support issue, or otherwise non-merchant-intent signal
- `NULL`: historical or not-yet-classified rows

### 6.2 Canonical Gmail thread categories

Every triaged Gmail thread must be exactly one of:
- `merchant_distress`
- `customer_support`
- `noise_system`

Semantics:
- `merchant_distress`: a merchant/operator is describing processor pain or payment-operational distress
- `customer_support`: human-written support or business inquiry that is not clearly merchant-distress
- `noise_system`: automated, irrelevant, system, self-test, or suppressed thread

Observed live production Gmail categories:
- only `noise_system`

Important note:
- the live Gmail merchant-distress path is proven, but the current mailbox contents are mostly system mail and internal validation threads
- the validated Northstar Cart Gmail thread now appears as `noise_system` / `test_thread=true` because later self-reply context changed the latest visible message state
- the underlying signal `696` remains the valid merchant-distress proof case

### 6.3 Historical classification layers still present in code

Older classification code also defines:
- `affiliate_distress`
- `processor_distress`
- `non_merchant`
- `merchant_operator`
- `consumer_or_irrelevant`

Canonical interpretation:
- these are historical or intermediate classifier outputs
- the live PayFlux operator-facing system now reasons primarily in:
  - `merchant_distress`
  - `consumer_complaint`
  - `customer_support`
  - `noise_system`

## 7. Opportunity Stage Taxonomy

### 7.1 Core `merchant_opportunities.status`

Observed current live statuses:
- `pending_review`
- `outreach_sent`
- `rejected`

Statuses referenced in code, even if not currently observed in production:
- `approved`
- `converted`

Canonical stage meanings:
- `pending_review`: opportunity exists and is waiting for operator decision
- `approved`: operator has approved the opportunity for downstream action
- `rejected`: operator or pipeline explicitly rejected the opportunity
- `outreach_sent`: outreach has been sent
- `converted`: the opportunity is considered closed as a customer conversion

### 7.2 Conversion-intelligence outcome taxonomy

The newer closed-loop outcome layer uses:
- `pending`
- `won`
- `lost`
- `ignored`

Live mapping from core opportunity status into conversion intelligence:
- `converted -> won`
- `rejected -> lost`
- `pending_review -> pending`
- `approved -> pending`
- `outreach_sent -> pending`

Observed live production conversion outcomes:
- `pending`
- `lost`
- `ignored`

Observed counts at reconstruction time:
- `won = 0`
- `lost = 2`
- `ignored = 1`
- `pending = 68`

Canonical rule:
- `merchant_opportunities.status` remains the operational workflow state
- `opportunity_conversion_intelligence.outcome_status` is the closed-loop reporting state

## 8. Operator Doctrine

The live system already implies a practical PayFlux doctrine:

### 8.1 Rules first

Deterministic rules are the default. Reasoning providers are for refinement, not for replacing obvious cases.

### 8.2 Merchant-distress first

Operator attention is for merchants with payment-operations pain, not generic support traffic.

### 8.3 Approval remains separate from intelligence

Gmail draft/send recommendations and opportunity advice can be generated automatically, but send authority remains approval-gated.

### 8.4 Suppress noise aggressively

System mail, self-tests, sandbox threads, and low-signal automation should be visible for audit if needed, but must not become signals or opportunities.

### 8.5 Scope truthfully

Operator replies should distinguish between:
- system-wide state
- queue-wide state
- specific lead state

Queue totals must not be presented as lead facts.

### 8.6 Pattern intelligence is advisory, not autonomous

Patterns should influence operator focus and prioritization, not trigger autonomous action without review.

## 9. PayFlux Decision Doctrine

These are the current decision principles PayFlux is already converging toward in the live system.

### 9.1 Priority doctrine

1. PayFlux prioritizes merchant distress tied to payment processors.
2. Immediate liquidity threats receive highest priority.
3. Merchants actively seeking processor alternatives are high-value signals.
4. Ecommerce merchants with frozen or restricted accounts are top targets.

In practice, the highest-signal live combinations are:
- `stripe` + `account_frozen`
- `stripe` + `payouts_delayed`
- merchant operators describing blocked payouts, held funds, disabled processing, or urgent processor migration needs

### 9.2 Explicitly excluded or downgraded signals

These should not be treated as strong PayFlux opportunities:
- consumer complaints
- general customer support
- spam or automated mail
- low-signal distress with no clear merchant/operator context

### 9.3 Decision consequence

When the system is uncertain, it should prefer:
- suppressing or downgrading low-signal/non-merchant cases
- preserving operator review on legitimate merchant-distress candidates
- enriching merchant, pattern, and conversion context before recommending action

## 10. Distress -> Strategy Mapping

This is the canonical operator playbook layer for how PayFlux should interpret distress once it is normalized.

### 10.1 Canonical mappings

- `account_frozen`
  - recommended strategy: urgent processor migration

- `payouts_delayed`
  - recommended strategy: payout acceleration strategy

- `reserve_hold`
  - recommended strategy: reserve negotiation strategy

- `verification_review`
  - recommended strategy: compliance remediation support

- `chargeback_issue`
  - recommended strategy: chargeback mitigation plus processor advisory

- `processor_switch_intent`
  - recommended strategy: merchant onboarding assistance

- `account_terminated`
  - recommended strategy: replacement processor recovery plan

- `onboarding_rejected`
  - recommended strategy: onboarding rescue and alternative processor placement

- `unknown`
  - recommended strategy: clarify the distress before escalating commitment

### 10.2 Usage rule

This mapping is for:
- operator briefings
- merchant profile interpretation
- opportunity reasoning context
- pattern actionability framing

It is not an autonomous action policy by itself.

## 11. Signal -> Merchant -> Opportunity Logic

### 9.1 Canonical pipeline

1. A source emits a candidate signal.
2. The signal is classified.
3. Consumer/non-merchant/noise is filtered or suppressed.
4. Merchant identity is extracted or linked.
5. The signal is attached to a merchant when possible.
6. If thresholds are met, an opportunity is created or updated.
7. Read models enrich the operator view:
   - Gmail thread intelligence
   - merchant intelligence profile
   - distress pattern cluster
   - opportunity conversion intelligence

### 9.2 Gmail-specific path

The proven Gmail path is:

`Gmail thread -> triage -> signal -> merchant -> opportunity -> approval-gated reply`

Eligibility rules used in the live Gmail layer:
- `reply_allowed = true` only for `merchant_distress` and `customer_support`
- `draft_recommended = true` only for `merchant_distress`
- `signal_eligible = true` only for `merchant_distress`
- `opportunity_eligible = true` only for `merchant_distress` with sufficiently strong confidence
- `test_thread = true` suppresses signal/opportunity creation

Observed live production state right now:
- all stored Gmail rows are currently `noise_system`
- all observed Gmail rows have `signal_eligible=false`
- all observed Gmail rows have `opportunity_eligible=false`
- the validated Gmail merchant-distress proof lives primarily in signal `696`, merchant `291`, and opportunity records tied to `northstarcart.co`

### 9.3 Pattern and profile layers

These do not replace the core pipeline.

They are read/write intelligence layers that summarize:
- recurring distress patterns across signals
- merchant context across merchants, signals, Gmail, and opportunities
- outcome quality across opportunity patterns

## 12. Worked Examples

### 10.1 Valid merchant-distress signal

Source:
- signal `696`
- source = `gmail`

Observed content markers:
- processor: `stripe`
- distress: `account_frozen`
- merchant candidate: `Northstar Cart`
- merchant domain candidate: `northstarcart.co`

Canonical interpretation:
- signal classification: `merchant_distress`
- processor: `stripe`
- distress type: `account_frozen`
- industry: `ecommerce`
- merchant: `Northstar Cart / northstarcart.co`
- opportunity-worthy: yes

### 10.2 Noise/system Gmail thread

Observed production examples:
- `Security alert` from `no-reply@accounts.google.com`
- `Your Google Workspace billing information was received` from `workspace@google.com`
- `Agent Flux Gmail Sandbox ...` from `hello@payflux.dev`

Canonical interpretation:
- thread category: `noise_system`
- signal eligible: no
- opportunity eligible: no

### 10.3 Disqualified / non-merchant case

Observed production example:
- Trustpilot PayPal complaint about face ID / app freezing

Canonical interpretation:
- signal classification: `consumer_complaint`
- not a merchant-distress lead
- should not become a PayFlux opportunity

### 10.4 High-value operator-facing merchant profile

Observed production merchant:
- `Northstarcart`
- domain: `northstarcart.co`
- linked signal: `Stripe` + `account_frozen`
- live operator-facing normalized display: `Northstar Cart (northstarcart.co)`

Canonical interpretation:
- high-value merchant profile
- tied to a rising processor-distress pattern
- deserves near-term operator review

### 10.5 Invalid opportunity-quality example

Observed production opportunity drift:
- distress topic = `Consumer Complaints`
- processor = `unknown`
- merchant domain unresolved or weak

Canonical interpretation:
- this is taxonomy drift, not a strong canonical PayFlux opportunity
- operator should rely on linked merchant/signal context before treating it as high-value

## 13. Canonical Reference Summary

If the operator assistant needs one compressed mental model, it should use this:

- PayFlux cares about merchants, not end-consumer complaints.
- The live signal core is `merchant_distress` vs `consumer_complaint`.
- The live Gmail core is `merchant_distress`, `customer_support`, `noise_system`.
- The live processor enum is the 9-value processor set plus `unknown`.
- The live primary distress enum is the 8-value distress set plus `unknown`.
- The live opportunity workflow is `pending_review -> approved/rejected/outreach_sent -> converted`.
- The live closed-loop reporting layer is `pending / won / lost / ignored`.
- Pattern intelligence, merchant profiles, and conversion intelligence are support layers for operator judgment.
- Gmail send behavior stays approval-gated.

## 14. Drift and Gaps To Watch

### 12.1 No legacy Markdown taxonomy doc

No existing `payflux_taxonomy.md`, `merchant_taxonomy.md`, `distress_taxonomy.md`, `signal_taxonomy.md`, or `operator_doctrine.md` was found.

The historical taxonomy lives in code, especially:
- `runtime/intelligence/entity_taxonomy.py`
- `runtime/intelligence/merchant_signal_classifier.py`
- `runtime/channels/gmail_triage.py`
- `runtime/reasoning/schemas.py`
- `runtime/intelligence/payflux_intelligence.py`

### 12.2 Taxonomy drift between old code and current operator system

Historical code still carries:
- `affiliate_distress`
- `processor_distress`
- `non_merchant`
- broad entity types such as `affiliate_network`, `bank`, `consumer_service`

The live operator system does not primarily reason in those categories anymore.

### 12.3 Opportunity distress-topic drift

`merchant_opportunities.distress_topic` is not canonically normalized yet and still contains free text like:
- `Unclassified merchant distress`
- `Consumer Complaints`

### 12.4 Merchant identity noise

Provisional merchant rows still show extraction artifacts like:
- `AM PST`
- `Risk Team`
- `Payouts`

These are not reliable canonical merchants.

### 12.5 Gmail runtime visibility drift

The validated Gmail merchant-distress case is proven end-to-end, but the current stored Gmail rows are all `noise_system` because the inbox is dominated by system mail and validation threads.

Canonical interpretation should therefore prefer:
- linked signals
- merchant resolution
- opportunities

over the current visible Gmail row distribution when reasoning about whether the Gmail path exists.
