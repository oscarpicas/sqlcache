package sqlcache

import (
	"regexp"
	"strconv"
)

var (
	attrRegexp = regexp.MustCompile(`(@cache-ttl|@cache-max-rows) (\d+)`)
)

type attributes struct {
	ttl     int
	maxRows int
}

func getAttrs(query string) (*attributes, error) {
	matches := attrRegexp.FindAllStringSubmatch(query, 2)
	if len(matches) != 2 {
		return nil, nil
	}

	var attrs attributes
	for _, match := range matches {
		if len(match) != 3 {
			return nil, nil
		}
		switch match[1] {
		case "@cache-ttl":
			ttl, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, err
			}
			attrs.ttl = ttl
		case "@cache-max-rows":
			maxRows, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, err
			}
			attrs.maxRows = maxRows
		}
	}

	return &attrs, nil
}
