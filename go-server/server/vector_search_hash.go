package main

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func hashVectorReference(ref *pb.VectorReference) (uint64, error) {
	if ref == nil {
		return 0, status.Error(codes.InvalidArgument, "vector reference is required")
	}

	hasher := fnv.New64a()
	switch value := ref.GetRef().(type) {
	case *pb.VectorReference_ObjectId:
		var data [5]byte
		data[0] = 'o'
		binary.LittleEndian.PutUint32(data[1:], uint32(value.ObjectId))
		_, _ = hasher.Write(data[:])
	case *pb.VectorReference_RawEmbedding:
		_, _ = hasher.Write([]byte{'r'})
		writeFloat32SliceHash(hasher, value.RawEmbedding.GetValues())
	default:
		return 0, status.Error(codes.InvalidArgument, "vector reference must set object_id or raw_embedding")
	}

	return hasher.Sum64(), nil
}

func writeFloat32SliceHash(hasher hash.Hash64, values []float32) {
	var data [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(data[:], math.Float32bits(value))
		_, _ = hasher.Write(data[:])
	}
}

func computeFilterHash(filters []qg.ParsedFilter, axes []qg.ParsedAxis) (uint64, error) {
	if len(filters) == 0 && len(axes) == 0 {
		return 0, nil
	}

	canonicalFilters := make([]string, 0, len(filters))
	for _, filter := range filters {
		entry, err := canonicalFilterHashEntry(filter)
		if err != nil {
			return 0, err
		}
		canonicalFilters = append(canonicalFilters, entry)
	}
	sort.Strings(canonicalFilters)

	canonicalAxes := make([]string, 0, len(axes))
	for _, axis := range axes {
		if strings.TrimSpace(axis.Type) == "" {
			continue
		}
		canonicalAxes = append(canonicalAxes, canonicalAxisHashEntry(axis))
	}
	sort.Strings(canonicalAxes)

	if len(canonicalFilters) == 0 && len(canonicalAxes) == 0 {
		return 0, nil
	}

	hasher := fnv.New64a()
	for _, entry := range canonicalFilters {
		_, _ = hasher.Write([]byte("f|"))
		_, _ = hasher.Write([]byte(entry))
		_, _ = hasher.Write([]byte{'\n'})
	}
	for _, entry := range canonicalAxes {
		_, _ = hasher.Write([]byte("a|"))
		_, _ = hasher.Write([]byte(entry))
		_, _ = hasher.Write([]byte{'\n'})
	}
	return hasher.Sum64(), nil
}

func canonicalAxisHashEntry(axis qg.ParsedAxis) string {
	return strings.ToLower(strings.TrimSpace(axis.Type)) + "|" + strconv.Itoa(axis.Id)
}

func canonicalFilterHashEntry(filter qg.ParsedFilter) (string, error) {
	filterType := strings.ToLower(strings.TrimSpace(filter.Type))
	if filterType == "" {
		return "", status.Error(codes.InvalidArgument, "filter type is required")
	}
	if isRangeFilterType(filterType) {
		return canonicalRangeFilterHashEntry(filterType, filter)
	}

	ids := append([]int(nil), filter.Ids...)
	sort.Ints(ids)
	var builder strings.Builder
	builder.WriteString(filterType)
	builder.WriteString("|ids=")
	for index, id := range ids {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.Itoa(id))
	}
	return builder.String(), nil
}

func canonicalRangeFilterHashEntry(filterType string, filter qg.ParsedFilter) (string, error) {
	if len(filter.Ids) != len(filter.Ranges) {
		return "", status.Errorf(
			codes.InvalidArgument,
			"range filter %s has %d ids but %d ranges",
			filterType,
			len(filter.Ids),
			len(filter.Ranges),
		)
	}

	type rangePair struct {
		id  int
		min string
		max string
	}

	pairs := make([]rangePair, 0, len(filter.Ids))
	for index, id := range filter.Ids {
		if len(filter.Ranges[index]) != 2 {
			return "", status.Errorf(
				codes.InvalidArgument,
				"range filter %s entry %d must have 2 bounds",
				filterType,
				index,
			)
		}
		pairs = append(pairs, rangePair{
			id:  id,
			min: filter.Ranges[index][0],
			max: filter.Ranges[index][1],
		})
	}

	sort.Slice(pairs, func(i int, j int) bool {
		if pairs[i].id != pairs[j].id {
			return pairs[i].id < pairs[j].id
		}
		if pairs[i].min != pairs[j].min {
			return pairs[i].min < pairs[j].min
		}
		return pairs[i].max < pairs[j].max
	})

	var builder strings.Builder
	builder.WriteString(filterType)
	builder.WriteString("|ranges=")
	for index, pair := range pairs {
		if index > 0 {
			builder.WriteByte(';')
		}
		builder.WriteString(strconv.Itoa(pair.id))
		builder.WriteByte(':')
		builder.WriteString(pair.min)
		builder.WriteByte(':')
		builder.WriteString(pair.max)
	}
	return builder.String(), nil
}

func isRangeFilterType(filterType string) bool {
	switch filterType {
	case "numrange", "alpharange", "daterange", "timerange", "timestamprange":
		return true
	default:
		return false
	}
}
