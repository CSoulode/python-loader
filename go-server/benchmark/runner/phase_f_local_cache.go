package runner

type phaseFLocalCache struct {
	seen map[string]map[string]struct{}
}

func newPhaseFLocalCache() *phaseFLocalCache {
	return &phaseFLocalCache{seen: make(map[string]map[string]struct{})}
}

func (c *phaseFLocalCache) Has(item phaseFRequest) bool {
	tab := phaseFTabKey(item)
	_, ok := c.seen[tab][item.key]
	return ok
}

func (c *phaseFLocalCache) Put(item phaseFRequest) {
	tab := phaseFTabKey(item)
	if c.seen[tab] == nil {
		c.seen[tab] = make(map[string]struct{})
	}
	c.seen[tab][item.key] = struct{}{}
}

func phaseFTabKey(item phaseFRequest) string {
	if item.tab == "" {
		return "main"
	}
	return item.tab
}
