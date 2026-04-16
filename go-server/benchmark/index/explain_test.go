package index

import "testing"

func TestUsesNamedIndex(t *testing.T) {
	t.Run("matches named index scan", func(t *testing.T) {
		plan := "Index Scan using idx_public_siglip2_tags_hnsw_half_cosine on public.siglip2_tags v"
		if !UsesNamedIndex(plan, "idx_public_siglip2_tags_hnsw_half_cosine") {
			t.Fatal("UsesNamedIndex returned false for matching index")
		}
	})

	t.Run("ignores unrelated index names", func(t *testing.T) {
		plan := "Index Scan using idx_public_siglip2_tags_hnsw_cosine on public.siglip2_tags v"
		if UsesNamedIndex(plan, "idx_public_siglip2_tags_hnsw_half_cosine") {
			t.Fatal("UsesNamedIndex returned true for non-matching index")
		}
	})

	t.Run("rejects empty inputs", func(t *testing.T) {
		if UsesNamedIndex("", "idx_public_siglip2_tags_hnsw_half_cosine") {
			t.Fatal("UsesNamedIndex returned true for empty plan")
		}
		if UsesNamedIndex("Index Scan using idx_name on t", "") {
			t.Fatal("UsesNamedIndex returned true for empty index name")
		}
	})
}
