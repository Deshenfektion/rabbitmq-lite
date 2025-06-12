package broker

import "strings"

const (
	wildcardSingle = "*"
	wildcardMulti  = "#"
	segmentSep     = "."
)

func matchTopic(pattern, routingKey string) bool {
	if pattern == wildcardMulti {
		return true
	}

	return matchSegments(strings.Split(pattern, segmentSep), strings.Split(routingKey, segmentSep))
}

func matchSegments(pattern, key []string) bool {
	matches := make([][]bool, len(pattern)+1)
	for i := range matches {
		matches[i] = make([]bool, len(key)+1)
	}

	matches[len(pattern)][len(key)] = true

	for i := len(pattern) - 1; i >= 0; i-- {
		for j := len(key); j >= 0; j-- {
			switch {
			case pattern[i] == wildcardMulti:
				matches[i][j] = matches[i+1][j] || (j < len(key) && matches[i][j+1])
			case j == len(key):
				matches[i][j] = false
			case pattern[i] == wildcardSingle || pattern[i] == key[j]:
				matches[i][j] = matches[i+1][j+1]
			default:
				matches[i][j] = false
			}
		}
	}

	return matches[0][0]
}
