package storage

import (
	"context"
	"fmt"
	"time"
)

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
	Confirm     bool      `json:"confirm"`
}

type HistoricalRepriceResult struct {
	Rows     int64     `json:"rows"`
	OldTotal float64   `json:"old_total_usd"`
	NewTotal float64   `json:"new_total_usd"`
	From     time.Time `json:"from"`
	Until    time.Time `json:"until"`
	Applied  bool      `json:"applied"`
	Policy   string    `json:"policy_version,omitempty"`
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
		args = append(args, req.ModelPrefix+"%")
		where = append(where, fmt.Sprintf("e.model LIKE $%d", len(args)))
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
	where, args, err := buildRepriceWhere(req)
	if err != nil {
		return HistoricalRepriceResult{}, err
	}
	query := fmt.Sprintf(`SELECT COUNT(*), COALESCE(SUM(e.cost_usd),0), COALESCE(SUM(%s),0)
		FROM usage_events e LEFT JOIN model_pricing p ON p.model=e.model
		WHERE %s`, costUSDExpr, where[5:])
	var result HistoricalRepriceResult
	result.From, result.Until, result.Policy = req.From, req.Until, req.Policy
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&result.Rows, &result.OldTotal, &result.NewTotal); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if !req.Confirm {
		return result, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HistoricalRepriceResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	update := fmt.Sprintf(`UPDATE usage_events u SET cost_usd = r.new_cost
		FROM (SELECT e.id, %s AS new_cost FROM usage_events e LEFT JOIN model_pricing p ON p.model=e.model WHERE %s) r
		WHERE u.id=r.id`, costUSDExpr, where[5:])
	if _, err := tx.ExecContext(ctx, update, args...); err != nil {
		return HistoricalRepriceResult{}, err
	}
	fromHour, untilHour := req.From.UTC().Truncate(time.Hour), req.Until.UTC().Truncate(time.Hour)
	if !fromHour.Before(untilHour) {
		untilHour = fromHour.Add(time.Hour)
	}
	if _, err := tx.ExecContext(ctx, rebuildHourLockSQL, fromHour, untilHour); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_hourly WHERE hour >= $1 AND hour < $2`, fromHour, untilHour); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, rebuildHourSQL, fromHour, untilHour); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, rebuildToolHourLockSQL, fromHour, untilHour); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, rebuildToolHourSQL, fromHour, untilHour); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if err := s.auditTx(ctx, tx, actor, "pricing.historical_reprice", "usage_events", map[string]any{
		"provider": req.Provider, "model": req.Model, "model_prefix": req.ModelPrefix,
		"from": req.From, "until": req.Until, "policy_version": req.Policy,
		"rows": result.Rows, "old_total_usd": result.OldTotal, "new_total_usd": result.NewTotal,
	}); err != nil {
		return HistoricalRepriceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return HistoricalRepriceResult{}, err
	}
	result.Applied = true
	s.parityHealthy.Store(false)
	s.servingRollups.Store(false)
	return result, nil
}
