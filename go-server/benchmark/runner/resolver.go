package runner

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type namedAxis struct {
	Axis       string
	Kind       string
	Candidates []string
}

type namedTagFilter struct {
	Kind             string
	TagsetCandidates []string
	ValueCandidates  []string
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

func resolveNamedAxis(ctx context.Context, db *sql.DB, spec namedAxis) (int32, error) {
	switch spec.Kind {
	case "tagset":
		return lookupTagsetID(ctx, db, spec.Candidates)
	case "node":
		return lookupNodeID(ctx, db, spec.Candidates)
	default:
		return 0, fmt.Errorf("unsupported axis kind %q", spec.Kind)
	}
}

func resolveNamedTagFilter(ctx context.Context, db *sql.DB, spec namedTagFilter) (int32, error) {
	switch spec.Kind {
	case "tag":
		return lookupTagID(ctx, db, spec.TagsetCandidates, spec.ValueCandidates)
	case "node":
		return lookupNodeID(ctx, db, spec.ValueCandidates)
	case "tagset":
		return lookupTagsetID(ctx, db, spec.ValueCandidates)
	default:
		return 0, fmt.Errorf("unsupported filter kind %q", spec.Kind)
	}
}

func lookupTagsetID(ctx context.Context, db *sql.DB, candidates []string) (int32, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, name FROM public.tagsets ORDER BY id ASC`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return matchNamedID(rows, candidates)
}

func lookupNodeID(ctx context.Context, db *sql.DB, candidates []string) (int32, error) {
	rows, err := db.QueryContext(ctx, `
SELECT n.id,
       COALESCE(a.name, ts.name::text, tm.name::text, d.name::text, num.name::text) AS node_name
FROM public.nodes n
JOIN public.tags t ON t.id = n.tag_id
LEFT JOIN public.alphanumerical_tags a ON a.id = t.id
LEFT JOIN public.timestamp_tags ts ON ts.id = t.id
LEFT JOIN public.time_tags tm ON tm.id = t.id
LEFT JOIN public.date_tags d ON d.id = t.id
LEFT JOIN public.numerical_tags num ON num.id = t.id
ORDER BY n.id ASC`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return matchNamedID(rows, candidates)
}

func lookupTagID(ctx context.Context, db *sql.DB, tagsetCandidates []string, valueCandidates []string) (int32, error) {
	tagsetID, err := lookupTagsetID(ctx, db, tagsetCandidates)
	if err != nil {
		return 0, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT t.id,
       COALESCE(a.name, ts.name::text, tm.name::text, d.name::text, num.name::text) AS tag_name
FROM public.tags t
LEFT JOIN public.alphanumerical_tags a ON a.id = t.id
LEFT JOIN public.timestamp_tags ts ON ts.id = t.id
LEFT JOIN public.time_tags tm ON tm.id = t.id
LEFT JOIN public.date_tags d ON d.id = t.id
LEFT JOIN public.numerical_tags num ON num.id = t.id
WHERE t.tagset_id = $1
ORDER BY t.id ASC`, tagsetID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return matchNamedID(rows, valueCandidates)
}

func matchNamedID(rows *sql.Rows, candidates []string) (int32, error) {
	type entry struct {
		id   int32
		name string
	}
	entries := make([]entry, 0)
	for rows.Next() {
		var item entry
		if err := rows.Scan(&item.id, &item.name); err != nil {
			return 0, err
		}
		entries = append(entries, item)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, candidate := range candidates {
		want := normalizeName(candidate)
		for _, item := range entries {
			if normalizeName(item.name) == want {
				return item.id, nil
			}
		}
	}
	for _, candidate := range candidates {
		want := normalizeName(candidate)
		for _, item := range entries {
			if strings.Contains(normalizeName(item.name), want) {
				return item.id, nil
			}
		}
	}
	return 0, fmt.Errorf("unable to resolve candidates %v", candidates)
}

func normalizeName(value string) string {
	return nonAlphaNum.ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "")
}
