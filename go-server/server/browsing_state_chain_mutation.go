package main

import (
	"context"
	"database/sql"
	"fmt"

	pb "m3.dataloader/dataloader"
)

func (s *DataLoaderServer) invalidateBrowsingStateDependencies(deps ...browsingStateDependency) int {
	if s == nil {
		return 0
	}
	removed := 0
	for _, dep := range uniqueBrowsingStateDependencies(deps) {
		removed += s.ensureBrowsingStateChain().InvalidateDependency(dep)
	}
	return removed
}

func (s *DataLoaderServer) invalidateBrowsingStateTagAndTagset(tagID int64, tagsetID int64) int {
	return s.invalidateBrowsingStateDependencies(
		browsingStateDependency{Kind: browsingStateDepTag, ID: fmt.Sprint(tagID)},
		browsingStateDependency{Kind: browsingStateDepTagset, ID: fmt.Sprint(tagsetID)},
	)
}

func (s *DataLoaderServer) createdTagResponse(tag *pb.Tag) *pb.Tag {
	if tag != nil {
		s.invalidateBrowsingStateTagAndTagset(tag.GetId(), tag.GetTagSetId())
	}
	return tag
}

func (s *DataLoaderServer) invalidateBrowsingStateTag(ctx context.Context, tagID int64) error {
	tagsetID, err := s.tagsetIDForTag(ctx, tagID)
	if err != nil {
		return err
	}
	s.invalidateBrowsingStateTagAndTagset(tagID, tagsetID)
	return nil
}

func (s *DataLoaderServer) invalidateBrowsingStateTags(ctx context.Context, tagIDs []int64) error {
	seen := make(map[int64]struct{}, len(tagIDs))
	for _, tagID := range tagIDs {
		if _, ok := seen[tagID]; ok {
			continue
		}
		seen[tagID] = struct{}{}
		if err := s.invalidateBrowsingStateTag(ctx, tagID); err != nil {
			return err
		}
	}
	return nil
}

func (s *DataLoaderServer) invalidateBrowsingStateNode(node *pb.Node) int {
	if node == nil {
		return 0
	}
	return s.invalidateBrowsingStateDependencies(nodeDependencies(node)...)
}

func (s *DataLoaderServer) tagsetIDForTag(ctx context.Context, tagID int64) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("browsing state invalidation database is not configured")
	}
	var tagsetID int64
	err := s.db.QueryRowContext(ctx, "SELECT tagset_id FROM public.tags WHERE id = $1", tagID).Scan(&tagsetID)
	if err == nil {
		return tagsetID, nil
	}
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("tag %d not found for browsing state invalidation", tagID)
	}
	return 0, fmt.Errorf("resolve tagset for tag %d: %w", tagID, err)
}

func nodeDependencies(node *pb.Node) []browsingStateDependency {
	deps := []browsingStateDependency{{Kind: browsingStateDepNode, ID: fmt.Sprint(node.GetId())}}
	if node.GetParentNodeId() != 0 {
		deps = append(deps, browsingStateDependency{Kind: browsingStateDepNode, ID: fmt.Sprint(node.GetParentNodeId())})
	}
	return deps
}
