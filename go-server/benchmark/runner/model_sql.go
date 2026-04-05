package runner

import (
	"fmt"
	"strings"
)

func modelTagsetCondition(model BenchmarkModel, alias string) string {
	return fmt.Sprintf("%s.tagset_id = (SELECT id FROM public.tagsets WHERE name = %s)", alias, sqlStringLiteral(model.TagsetName))
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
