package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type phaseFInvalidationSummary struct {
	Total               int
	AncestorCount       int
	NotModified         int
	Fresh               int
	PayloadSaved        int
	MissedInvalidations int
}

type phaseFPerfRowInput struct {
	Plan   phaseFSessionPlan
	Index  int
	Item   phaseFRequest
	Result phaseFBenchResult
	Nodes  int
}

func phaseFPerfRow(input phaseFPerfRowInput) []string {
	return []string{
		input.Plan.Type, input.Plan.ID, strconv.Itoa(input.Index), input.Item.complexity,
		strconv.Itoa(input.Item.vectorDimCount), phaseFResultDelta(input.Item, input.Result),
		input.Result.LookupPath, input.Result.Reusable,
		strconv.FormatBool(input.Result.L0Hit), input.Result.L1PathHit,
		FormatFloat(input.Result.ElapsedMS), strconv.Itoa(input.Nodes), strconv.Itoa(input.Nodes * 18),
	}
}

func phaseFResultDelta(item phaseFRequest, result phaseFBenchResult) string {
	if item.deltaKind != "" {
		return item.deltaKind
	}
	return result.DeltaKind
}

type phaseFInvalidationRowInput struct {
	Runner           *BenchRunner
	Dataset          DatasetID
	Scenario         string
	Summary          phaseFInvalidationSummary
	InvalidationType string
	DepKind          string
	DepKey           string
	Counts           phaseFInvalidationCounts
}

func newPhaseFInvalidationRowInput(
	runner *BenchRunner,
	dataset DatasetID,
	scenario string,
	summary phaseFInvalidationSummary,
	counts phaseFInvalidationCounts,
) phaseFInvalidationRowInput {
	return phaseFInvalidationRowInput{
		Runner: runner, Dataset: dataset, Scenario: scenario, Summary: summary, Counts: counts,
	}
}

func phaseFInvalidationRow(input phaseFInvalidationRowInput) []string {
	survived := input.Counts.Before - input.Counts.Removed
	return []string{
		input.Runner.opts.DatasetLabel(input.Dataset), input.Runner.opts.DatasetSizeLabel(input.Dataset),
		"grpc", input.Scenario, strconv.Itoa(input.Summary.Total),
		strconv.Itoa(input.Summary.AncestorCount), strconv.Itoa(input.Summary.NotModified),
		strconv.Itoa(input.Summary.Fresh), strconv.Itoa(input.Summary.PayloadSaved),
		input.InvalidationType, input.DepKind, input.DepKey, strconv.Itoa(input.Counts.Before),
		strconv.Itoa(input.Counts.Removed), strconv.Itoa(survived),
		FormatFloat(float64(survived) / float64(input.Counts.Before)),
		strconv.Itoa(input.Summary.MissedInvalidations),
	}
}

type phaseFInvalidationCounts struct {
	Before  int
	Removed int
}

type phaseFInvalidationRequest struct {
	Scope string
	Kind  string
	ID    string
}

func phaseFInvalidate(ctx context.Context, session *ActiveSession, request phaseFInvalidationRequest) (int, error) {
	query := url.Values{"scope": []string{request.Scope}}
	query.Set("kind", request.Kind)
	query.Set("id", request.ID)
	endpoint := fmt.Sprintf(
		"http://127.0.0.1:%d/debug/cache/browsing-state/invalidate?%s",
		session.Runtime.Ports.ServerHTTP,
		query.Encode(),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader("{}"))
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return decodePhaseFInvalidateResponse(resp)
}

func decodePhaseFInvalidateResponse(resp *http.Response) (int, error) {
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("phase F invalidate status %d", resp.StatusCode)
	}
	var body struct {
		Removed int `json:"removed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.Removed, nil
}
