package querygen

import (
	"fmt"
	"strings"
)

// GenerateSQLQueryForState builds the SQL for the “state” endpoint.
func GenerateSQLQueryForState(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	numberOfAdditionalFilters := len(filtersList)
	totalNumberOfFilters := numberOfAdditionalFilters
	if xType != "" {
		totalNumberOfFilters++
	}
	if yType != "" {
		totalNumberOfFilters++
	}
	if zType != "" {
		totalNumberOfFilters++
	}
	numberOfFilters := 0

	// No filters or axes: return the simple base query
	if totalNumberOfFilters == 0 {
		return "" +
			"select X.idx as x, X.idy as y, X.idz as z, X.object_id as id, " +
			"O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, X.cnt as count " +
			"from (select 1 as idx, 1 as idy, 1 as idz, max(R1.id) as object_id, count(*) as cnt " +
			"from medias R1 group by idx, idy, idz) X " +
			"join medias O on X.object_id = O.id;"
	}

	var front, middle, end strings.Builder
	front.WriteString(
		"select X.idx as x, X.idy as y, X.idz as z, X.object_id as id, " +
			"O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, X.cnt as count from (select ",
	)
	middle.WriteString(" from (")
	end.WriteString(" group by idx, idy, idz")

	// X axis
	numberOfFilters++
	if xType == "" {
		front.WriteString("1 as idx, ")
	} else {
		front.WriteString(fmt.Sprintf("R%d.id as idx, ", numberOfFilters))
		middle.WriteString(generateAxisQueryForState(xType, xVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	// Y axis
	numberOfFilters++
	if yType == "" {
		front.WriteString("1 as idy, ")
	} else {
		front.WriteString(fmt.Sprintf("R%d.id as idy, ", numberOfFilters))
		middle.WriteString(generateAxisQueryForState(yType, yVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	// Z axis
	numberOfFilters++
	if zType == "" {
		front.WriteString("1 as idz, ")
	} else {
		front.WriteString(fmt.Sprintf("R%d.id as idz, ", numberOfFilters))
		middle.WriteString(generateAxisQueryForState(zType, zVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	front.WriteString("max(R1.object_id) as object_id, count(distinct R1.object_id) as cnt ")
	end.WriteString(") X join medias O on X.object_id = O.id;")

	// Additional filters
	for _, filter := range filtersList {
		numberOfFilters++
		middle.WriteString(generateFilterQueryForState(filter, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	return front.String() + middle.String() + end.String()
}

// GenerateSQLQueryForCell builds the SQL for the “cell” endpoint.
func GenerateSQLQueryForCell(
	xType string, xVertexID int,
	yType string, yVertexID int,
	zType string, zVertexID int,
	filtersList []ParsedFilter,
) string {
	numberOfAdditionalFilters := len(filtersList)
	totalNumberOfFilters := numberOfAdditionalFilters
	if xType != "" {
		totalNumberOfFilters++
	}
	if yType != "" {
		totalNumberOfFilters++
	}
	if zType != "" {
		totalNumberOfFilters++
	}
	numberOfFilters := 0

	// No filters or axes: simple media list
	if totalNumberOfFilters == 0 {
		return "select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI from medias O;"
	}

	var front, middle, end strings.Builder
	front.WriteString(
		"select distinct O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, TS.name as T from (select R1.object_id ",
	)
	middle.WriteString(" from (")
	end.WriteString(
		") X join medias O on X.object_id = O.id " +
			"join taggings R2 on O.id = R2.object_id " +
			"join timestamp_tags TS on R2.tag_id = TS.id " +
			"join tagsets S on TS.tagset_id = S.id " +
			"where S.name = 'Timestamp UTC' order by TS.name;",
	)

	// X axis
	if xType != "" {
		numberOfFilters++
		middle.WriteString(generateAxisQueryForCell(xType, xVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	// Y axis
	if yType != "" {
		numberOfFilters++
		middle.WriteString(generateAxisQueryForCell(yType, yVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	// Z axis
	if zType != "" {
		numberOfFilters++
		middle.WriteString(generateAxisQueryForCell(zType, zVertexID, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	// Additional filters
	for _, filter := range filtersList {
		numberOfFilters++
		middle.WriteString(generateFilterQueryForCell(filter, numberOfFilters))
		if numberOfFilters < totalNumberOfFilters {
			middle.WriteString(" join (")
		}
	}

	return front.String() + middle.String() + end.String()
}

// GenerateSQLQueryForTimeline builds the SQL for the “timeline” endpoint.
func GenerateSQLQueryForTimeline(filtersList []ParsedFilter) string {
	totalNumberOfFilters := len(filtersList)

	// If not exactly one filter, return simple media list
	if totalNumberOfFilters != 1 {
		return "select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI from medias O;"
	}

	var front, middle, end strings.Builder
	front.WriteString(
		"select O.id as Id, O.file_uri as fileURI, O.thumbnail_uri as thumbnailURI, TS1.name as T ",
	)
	middle.WriteString(
		"from medias O join taggings R1 on O.id = R1.object_id " +
			"join timestamp_tags TS1 on R1.tag_id = TS1.id " +
			"join tagsets S on TS1.tagset_id = S.id " +
			"join timestamp_tags TS2 on TS1.tagset_id = TS2.tagset_id " +
			"and TS1.name between TS2.name - interval '30 minutes' " +
			"and TS2.name + interval '30 minutes' " +
			"join taggings R2 on TS2.id = R2.tag_id where S.name = 'Timestamp UTC' and R2.object_id = ",
	)
	end.WriteString(" order by TS1.name;")

	// Exactly one filter
	return front.String() + middle.String() + fmt.Sprintf("%d", filtersList[0].Ids[0]) + end.String()
}

// Helpers

func generateAxisQueryForState(axisType string, vertexID, filterNum int) string {
	switch axisType {
	case "node":
		return fmt.Sprintf(
			" select N.object_id, N.node_id as id from nodes_taggings N where N.parentnode_id = %d) R%d ",
			vertexID, filterNum,
		)
	case "tagset":
		return fmt.Sprintf(
			" select T.object_id, T.tag_id as id from tagsets_taggings T where T.tagset_id = %d) R%d ",
			vertexID, filterNum,
		)
	default:
		return ""
	}
}

func generateAxisQueryForCell(axisType string, vertexID, filterNum int) string {
	switch axisType {
	case "node":
		return fmt.Sprintf(
			" select N.object_id from nodes_taggings N where N.node_id = %d) R%d ",
			vertexID, filterNum,
		)
	case "tag":
		return fmt.Sprintf(
			" select R.object_id from taggings R where R.tag_id = %d) R%d ",
			vertexID, filterNum,
		)
	default:
		return ""
	}
}

func generateFilterQueryForState(filter ParsedFilter, filterNum int) string {
	switch filter.Type {
	case "node":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select N.object_id from nodes_taggings N where N.node_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select N.object_id from nodes_taggings N where N.node_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "tagset":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select T.object_id from tagsets_taggings T where T.tagset_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select T.object_id from tagsets_taggings T where T.tagset_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "tag":
		if len(filter.Ids) == 1 {
			return fmt.Sprintf(
				" select R.object_id from taggings R where R.tag_id = %d) R%d",
				filter.Ids[0], filterNum,
			)
		}
		return fmt.Sprintf(
			" select R.object_id from taggings R where R.tag_id in %s) R%d",
			generateIdList(filter), filterNum,
		)
	case "numrange":
		return fmt.Sprintf(
			" select R.object_id from numerical_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, ""), filterNum,
		)
	case "alpharange":
		return fmt.Sprintf(
			" select R.object_id from alphanumerical_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "daterange":
		return fmt.Sprintf(
			" select R.object_id from date_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "timerange":
		return fmt.Sprintf(
			" select R.object_id from time_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	case "timestamprange":
		return fmt.Sprintf(
			" select R.object_id from timestamp_tags T join taggings R on T.id = R.tag_id where %s) R%d",
			generateRangeList(filter, "'"), filterNum,
		)
	default:
		return ""
	}
}

// generateFilterQueryForCell is identical to generateFilterQueryForState
func generateFilterQueryForCell(filter ParsedFilter, filterNum int) string {
	return generateFilterQueryForState(filter, filterNum)
}

func generateIdList(filter ParsedFilter) string {
	var b strings.Builder
	b.WriteString("(")
	for i, id := range filter.Ids {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%d", id))
	}
	b.WriteString(")")
	return b.String()
}

func generateRangeList(filter ParsedFilter, quote string) string {
	var b strings.Builder
	b.WriteString("(")
	for i := range filter.Ids {
		if i > 0 {
			b.WriteString(") or (")
		}
		min := filter.Ranges[i][0]
		max := filter.Ranges[i][1]
		b.WriteString(fmt.Sprintf(
			"T.tagset_id = %d and T.name between %s%s%s and %s%s%s",
			filter.Ids[i], quote, min, quote, quote, max, quote,
		))
	}
	b.WriteString(")")
	return b.String()
}
