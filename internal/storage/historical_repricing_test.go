package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/redhat-et/pricetag-metering/internal/pricing"
)

func TestBuildRepriceWhereTreatsPrefixMetacharactersLiterally(t *testing.T) {
	req := HistoricalRepriceRequest{
		ModelPrefix: "gemini_%",
		From:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Until:       time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	where, args, err := buildRepriceWhere(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(where, " LIKE ") {
		t.Fatalf("prefix scope must not use wildcard LIKE: %s", where)
	}
	if got := args[2]; got != req.ModelPrefix {
		t.Fatalf("prefix argument was transformed: got %v, want %q", got, req.ModelPrefix)
	}
}

func TestDiscountPolicyHashBindsRules(t *testing.T) {
	base := pricing.DefaultDiscountPolicy()
	first, err := discountPolicyHash(base)
	if err != nil {
		t.Fatal(err)
	}
	base.Rules[0].Multiplier = 0.75
	second, err := discountPolicyHash(base)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("policy hash did not change when policy rules changed")
	}
}
