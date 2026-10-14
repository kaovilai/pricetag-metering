package handler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/redhat-et/pricetag-metering/internal/storage"
)

type HistoricalRepricingHandler struct{ store *storage.Store }

func NewHistoricalRepricingHandler(store *storage.Store) *HistoricalRepricingHandler {
	return &HistoricalRepricingHandler{store: store}
}

func (h *HistoricalRepricingHandler) Handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Provider    string `json:"provider"`
		Model       string `json:"model"`
		ModelPrefix string `json:"model_prefix"`
		From        string `json:"from"`
		Until       string `json:"until"`
		Policy      string `json:"policy_version"`
		PolicyHash  string `json:"policy_hash"`
		Confirm     bool   `json:"confirm"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	from, err := time.Parse(time.RFC3339, strings.TrimSpace(req.From))
	if err != nil {
		http.Error(w, "from must be RFC3339", http.StatusBadRequest)
		return
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(req.Until))
	if err != nil {
		http.Error(w, "until must be RFC3339", http.StatusBadRequest)
		return
	}
	result, err := h.store.RepriceHistorical(r.Context(), quotaAdminActor(r), storage.HistoricalRepriceRequest{
		Provider: req.Provider, Model: req.Model, ModelPrefix: req.ModelPrefix,
		From: from, Until: until, Policy: req.Policy, PolicyHash: req.PolicyHash, Confirm: req.Confirm,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}
