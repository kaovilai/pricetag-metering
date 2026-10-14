package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redhat-et/pricetag-metering/internal/pricing"
)

const maxHistoricalRepriceRows int64 = 100_000

// HistoricalRepriceRequest scopes a deterministic recomputation from token
// counts and the current effective model_pricing rows. It never multiplies an
// existing cost_usd value, so repeating the operation is idempotent.
type HistoricalRepriceRequest struct {
	Provider    string    `json:"provider"`
	Model       string    `json:"model"`
	ModelPrefix string    `json:"model_prefix"`
	From        time.Time `json:"from"`
	Until       time.Time `json:"until"`
	Policy      string    `json:"policy_version"`
	PolicyHash  string    `json:"policy_hash"`
	Confirm     bool      `json:"confirm"`
}

type HistoricalRepriceResult struct {
	Rows       int64     `json:"rows"`
	OldTotal   float64   `json:"old_total_usd"`
	NewTotal   float64   `json:"new_total_usd"`
	From       time.Time `json:"from"`
	Until      time.Time `json:"until"`
	Applied    bool      `json:"applied"`
	Policy     string    `json:"policy_version,omitempty"`
	PolicyHash string    `json:"policy_hash,omitempty"`
	MaxRows    int64     `json:"max_rows"`
}

func discountPolicyHash(policy pricing.DiscountPolicy) (string, error) {
	raw, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("marshal discount policy: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func buildRepriceWhere(req HistoricalRepriceRequest) (string, []any, error) {
	if req.From.IsZero() || req.Until.IsZero() || !req.From.Before(req.Until) {
		return "", nil, fmt.Errorf("from and until are required and until must be after from")
	}
	if req.Until.Sub(req.From) > 366*24*time.Hour {
		return "", nil, fmt.Errorf("repricing range cannot exceed 366 days")
	}
	if req.Provider == "" && req.Model == "" && req.ModelPrefix == "" {
		return "", nil, fmt.Errorf("provider, model, or model_prefix is required")
	}
	where := []string{"e.timestamp >= $1", "e.timestamp < $2"}
	args := []any{req.From, req.Until}
	if req.Provider != "" {
		args = append(args, req.Provider)
		where = append(where, fmt.Sprintf("e.provider = $%d", len(args)))
	}
	if req.Model != "" {
		args = append(args, req.Model)
		where = append(where, fmt.Sprintf("e.model = $%d", len(args)))
	}
	if req.ModelPrefix != "" {
		// Compare the prefix literally; LIKE would interpret user-supplied
		// '%' and '_' as wildcards and could reprice unintended models.
		args = append(args, req.ModelPrefix)
		where = append(where, fmt.Sprintf("LEFT(e.model, LENGTH($%d)) = $%d", len(args), len(args)))
	}
	return " AND " + joinSQL(where), args, nil
}

func joinSQL(parts []string) string {
	out := ""
	for i, part := range parts {
		if i > 0 {
			out += " AND "
		}
		out += part
	}
	return out
}

func (s *Store) RepriceHistorical(ctx context.Context, actor string, req HistoricalRepriceRequest) (HistoricalRepriceResult, error) {
	if req.Policy == "" {
		return HistoricalRepriceResult{}, fmt.Errorf("policy_version is required")
	}
	activePolicy, err := s.GetPricingDiscountPolicy(ctx)
	if err != nil {
		return HistoricalRepriceResult{}, fmt.Errorf("load active pricing policy: %w", err)
	}
	if req.Policy != activePolicy.Version {
		return HistoricalRepriceResult{}, fmt.Errorf("policy_version %q is not the active policy %q", req.Policy, activePolicy.Version)
	}
	policyHash, err := discountPolicyHash(activePolicy)
	if err != nil {
		return HistoricalRepriceResult{}, err
	}
	if req.Confirm && req.PolicyHash != policyHash {
		return HistoricalRepriceResult{}, fmt.Errorf("policy_hash does not match the active policy; run a new dry run")
	}
	where, args, err := buildRepriceWhere(req)
	if err != nil {
		return HistoricalRepriceResult{}, err
	}
	query := fmt.Sprintf(`SELECT COUNT(*), COALESCE(SUM(e.cost_usd),0), COALESCE(SUM(%s),0)
		FROM usage_events e LEFT JOIN model_pricing p ON p.model=e.model
		WHERE %s`, costUSDExpr, where[5:])
	var result HistoricalRepriceResult
	result.From, result.Until, result.Policy = req.From, req.Until, req.Policy
	result.PolicyHash, result.MaxRows = policyHash, maxHistoricalRepriceRows
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&result.Rows, &result.OldTotal, &result.NewTotal); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if !req.Confirm {
		return result, nil
	}
	if result.Rows > maxHistoricalRepriceRows {
		return HistoricalRepriceResult{}, fmt.Errorf("repricing affects %d rows; maximum per operation is %d", result.Rows, maxHistoricalRepriceRows)
	}

	fromHour, untilHour := req.From.UTC().Truncate(time.Hour), req.Until.UTC().Truncate(time.Hour)
	if req.Until.UTC().After(untilHour) {
		untilHour = untilHour.Add(time.Hour)
	}
	if !fromHour.Before(untilHour) {
		untilHour = fromHour.Add(time.Hour)
	}
	// Apply one UTC hour per transaction. This bounds locks, WAL, and rollback
	// scope even when an operator requests a long historical range.
	for hour := fromHour; hour.Before(untilHour); hour = hour.Add(time.Hour) {
		chunkUntil := hour.Add(time.Hour)
		if chunkUntil.After(untilHour) {
			chunkUntil = untilHour
		}
		chunkReq := req
		if chunkReq.From.Before(hour) {
			chunkReq.From = hour
		}
		if chunkReq.Until.After(chunkUntil) {
			chunkReq.Until = chunkUntil
		}
		chunkWhere, chunkArgs, err := buildRepriceWhere(chunkReq)
		if err != nil {
			return HistoricalRepriceResult{}, err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return HistoricalRepriceResult{}, err
		}
		update := fmt.Sprintf(`UPDATE usage_events u SET cost_usd = r.new_cost
			FROM (SELECT e.id, %s AS new_cost FROM usage_events e LEFT JOIN model_pricing p ON p.model=e.model WHERE %s) r
			WHERE u.id=r.id`, costUSDExpr, chunkWhere[5:])
		if _, err = tx.ExecContext(ctx, update, chunkArgs...); err == nil {
			_, err = tx.ExecContext(ctx, rebuildHourLockSQL, hour, chunkUntil)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM usage_hourly WHERE hour >= $1 AND hour < $2`, hour, chunkUntil)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, rebuildHourSQL, hour, chunkUntil)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, rebuildToolHourLockSQL, hour, chunkUntil)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, rebuildToolHourSQL, hour, chunkUntil)
		}
		if err == nil {
			err = s.auditTx(ctx, tx, actor, "pricing.historical_reprice", "usage_events", map[string]any{
				"provider": req.Provider, "model": req.Model, "model_prefix": req.ModelPrefix,
				"from": chunkReq.From, "until": chunkReq.Until, "policy_version": req.Policy,
				"rows": result.Rows, "old_total_usd": result.OldTotal, "new_total_usd": result.NewTotal,
			})
		}
		if err != nil {
			_ = tx.Rollback()
			return HistoricalRepriceResult{}, fmt.Errorf("reprice hour %s: %w", hour.Format(time.RFC3339), err)
		}
		if err = tx.Commit(); err != nil {
			return HistoricalRepriceResult{}, fmt.Errorf("commit reprice hour %s: %w", hour.Format(time.RFC3339), err)
		}
	}
	result.Applied = true
	s.parityHealthy.Store(false)
	s.servingRollups.Store(false)
	return result, nil
}
