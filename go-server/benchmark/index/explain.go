package index

import "strings"

func UsesNamedIndex(plan string, indexName string) bool {
	trimmedPlan := strings.TrimSpace(plan)
	trimmedIndex := strings.TrimSpace(indexName)
	if trimmedPlan == "" || trimmedIndex == "" {
		return false
	}
	return strings.Contains(trimmedPlan, "using "+trimmedIndex+" ") ||
		strings.Contains(trimmedPlan, "using "+trimmedIndex+"\n") ||
		strings.Contains(trimmedPlan, trimmedIndex)
}
