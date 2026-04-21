"""
Merchant Identity Unit Tests
Validates domain extraction, normalization, company name extraction, and resolution logic.
"""
import sys, os, re, importlib.util
sys.path.insert(0, os.path.dirname(__file__))

# Import pure functions directly to avoid triggering the sqlalchemy dependency
# which isn't installed on the dev machine
spec = importlib.util.spec_from_file_location("merchant_identity", os.path.join(os.path.dirname(__file__), "merchant_identity.py"))
loader = spec.loader

# Read and exec only the pure functions we need for testing
_source = open(os.path.join(os.path.dirname(__file__), "merchant_identity.py")).read()

# Extract constants and pure functions without importing the module
_globals = {"re": re, "__name__": "merchant_identity"}
# Execute just the regex/constants/function definitions, skip the sqlalchemy imports
_lines = _source.split("\n")
_filtered = []
_skip = False
for line in _lines:
    if "from sqlalchemy" in line or "from config." in line:
        continue
    if line.startswith("engine =") or line.startswith("logger ="):
        continue
    _filtered.append(line)
exec("\n".join(_filtered), _globals)

extract_domains = _globals["extract_domains"]
extract_company_names = _globals["extract_company_names"]
normalize_domain = _globals["normalize_domain"]
_domain_to_name = _globals["_domain_to_name"]

PASS = 0
FAIL = 0


def assert_eq(label, actual, expected):
    global PASS, FAIL
    if actual == expected:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}: expected {expected!r}, got {actual!r}")


def assert_in(label, item, collection):
    global PASS, FAIL
    if item in collection:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}: {item!r} not found in {collection!r}")


def assert_not_in(label, item, collection):
    global PASS, FAIL
    if item not in collection:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}: {item!r} should NOT be in {collection!r}")


def test_domain_extraction():
    print("\n── Domain Extraction ──")

    domains = extract_domains("Stripe froze payouts for my store example-store.com")
    assert_in("extracts example-store.com", "example-store.com", domains)

    domains = extract_domains("Check https://www.bestwidgets.co.uk for details")
    assert_in("extracts bestwidgets.co.uk (strips www)", "bestwidgets.co.uk", domains)

    domains = extract_domains("Visit shop.mymerchant.com or app.mymerchant.com")
    assert_in("extracts mymerchant.com (strips shop)", "mymerchant.com", domains)

    domains = extract_domains("Posted on https://www.reddit.com/r/stripe")
    assert_not_in("filters reddit.com", "reddit.com", domains)

    domains = extract_domains("Check github.com/stripe/stripe-python")
    assert_not_in("filters github.com", "github.com", domains)

    domains = extract_domains("No domain in this text at all")
    assert_eq("no domains found", domains, [])


def test_domain_normalization():
    print("\n── Domain Normalization ──")

    assert_eq("strips www", normalize_domain("www.example.com"), "example.com")
    assert_eq("strips shop", normalize_domain("shop.example.com"), "example.com")
    assert_eq("strips app", normalize_domain("app.mystore.io"), "mystore.io")
    assert_eq("lowercases", normalize_domain("MyStore.COM"), "mystore.com")
    assert_eq("strips trailing dot", normalize_domain("example.com."), "example.com")
    assert_eq("preserves subdomains beyond known ones", normalize_domain("api.example.com"), "api.example.com")
    assert_eq("handles None", normalize_domain(None), None)


def test_company_name_extraction():
    print("\n── Company Name Extraction ──")

    names = extract_company_names("Stripe froze payouts for Quick Commerce Inc")
    assert_in("extracts Quick Commerce Inc", "Quick Commerce Inc", names)

    names = extract_company_names("no company names here at all lowercase")
    assert_eq("no names in lowercase text", names, [])


def test_domain_to_name():
    print("\n── Domain to Name Conversion ──")

    assert_eq("example-store.com → Example Store", _domain_to_name("example-store.com"), "Example Store")
    assert_eq("my_shop.io → My Shop", _domain_to_name("my_shop.io"), "My Shop")
    assert_eq("bestwidgets.co.uk → Bestwidgets", _domain_to_name("bestwidgets.co.uk"), "Bestwidgets")


def test_empty_inputs():
    print("\n── Empty / None Input ──")

    assert_eq("empty string domains", extract_domains(""), [])
    assert_eq("None domains", extract_domains(None), [])
    assert_eq("empty string names", extract_company_names(""), [])
    assert_eq("None names", extract_company_names(None), [])


if __name__ == "__main__":
    print("=" * 50)
    print("MERCHANT IDENTITY TEST SUITE")
    print("=" * 50)

    test_domain_extraction()
    test_domain_normalization()
    test_company_name_extraction()
    test_domain_to_name()
    test_empty_inputs()

    print(f"\n{'=' * 50}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    print(f"{'=' * 50}")

    sys.exit(1 if FAIL > 0 else 0)
