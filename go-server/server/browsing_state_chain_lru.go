package main

import "time"

func (c *BrowsingStateChain) touchLocked(node *StateNode, now time.Time) {
	if node == nil {
		return
	}
	node.LastAccessAt = now
	for current := node; current != nil; current = c.nodes[current.ParentID] {
		current.SubtreeLastAccess = now
	}
	if node.lruElementAttached {
		for element := c.lru.Front(); element != nil; element = element.Next() {
			if element.Value == node.ID {
				c.lru.MoveToFront(element)
				return
			}
		}
	}
}

func (c *BrowsingStateChain) addLeafLocked(node *StateNode) {
	if node == nil || node.ChildCount != 0 || node.lruElementAttached {
		return
	}
	c.lru.PushFront(node.ID)
	node.lruElementAttached = true
}

func (c *BrowsingStateChain) removeLeafLocked(node *StateNode) {
	if node == nil || !node.lruElementAttached {
		return
	}
	for element := c.lru.Front(); element != nil; element = element.Next() {
		if element.Value == node.ID {
			c.lru.Remove(element)
			node.lruElementAttached = false
			return
		}
	}
}

func (c *BrowsingStateChain) purgeExpiredLocked(now time.Time) {
	if c.ttl <= 0 {
		return
	}
	for id, node := range c.nodes {
		if now.Sub(node.CreatedAt) < c.ttl {
			continue
		}
		c.removeSubtreeLocked(id)
		c.expires.Add(1)
	}
}

func (c *BrowsingStateChain) evictOverflowLocked() {
	for len(c.nodes) > c.maxNodes {
		element := c.lru.Back()
		if element == nil {
			return
		}
		id, _ := element.Value.(StateID)
		c.removeNodeLocked(id)
		c.evictions.Add(1)
	}
}

func (c *BrowsingStateChain) removeSubtreeLocked(id StateID) {
	_ = c.removeSubtreeCountLocked(id)
}

func (c *BrowsingStateChain) removeSubtreeCountLocked(id StateID) int {
	if c.nodes[id] == nil {
		return 0
	}
	removed := 1
	children := c.children[id]
	for childID := range children {
		removed += c.removeSubtreeCountLocked(childID)
	}
	c.removeNodeLocked(id)
	return removed
}

func (c *BrowsingStateChain) removeNodeLocked(id StateID) {
	node := c.nodes[id]
	if node == nil {
		return
	}
	c.removeLeafLocked(node)
	delete(c.children, id)
	delete(c.nodes, id)
	c.unlinkFromParentLocked(node)
}

func (c *BrowsingStateChain) unlinkFromParentLocked(node *StateNode) {
	if node.ParentID == "" || c.children[node.ParentID] == nil {
		return
	}
	delete(c.children[node.ParentID], node.ID)
	parent := c.nodes[node.ParentID]
	if parent == nil || parent.ChildCount <= 0 {
		return
	}
	parent.ChildCount--
	c.addLeafLocked(parent)
}
