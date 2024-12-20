package enc

import (
	"iter"
)

func AnyToAny(a any) any {
	switch t := a.(type) {

	case uint64:
		return int64(t)

	default:
		return a

	}
}

func MapsToMaps(
	original iter.Seq2[map[string]any, error],
) iter.Seq2[map[string]any, error] {
	buf := map[string]any{}
	return func(yield func(map[string]any, error) bool) {
		for m, e := range original {
			clear(buf)
			if nil == e {
				for key, val := range m {
					var converted any = AnyToAny(val)
					buf[key] = converted
				}
			}
			if !yield(buf, e) {
				return
			}
		}
	}
}
