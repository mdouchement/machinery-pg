package machinerypg

// Metrics holds any kind of machinery-pg metrics.
// It ignores all errors.
type Metrics map[string]interface{}

func (m Metrics) String(key string) string {
	if value, ok := m[key].(string); ok {
		return value
	}
	return ""
}

func (m Metrics) Bool(key string) bool {
	if value, ok := m[key].(bool); ok {
		return value
	}
	return false
}

func (m Metrics) Int(key string) int {
	if value, ok := m[key].(int); ok {
		return value
	}
	return 0
}

func (m Metrics) Float64(key string) float64 {
	if value, ok := m[key].(float64); ok {
		return value
	}
	return 0.0
}

func (m Metrics) Uint64(key string) uint64 {
	if value, ok := m[key].(uint64); ok {
		return value
	}
	return 0
}
