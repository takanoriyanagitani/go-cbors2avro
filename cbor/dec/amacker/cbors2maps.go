package cbors2maps

import (
	"bufio"
	"io"
	"iter"
	"os"

	fc "github.com/fxamacker/cbor/v2"

	. "github.com/takanoriyanagitani/go-cbors2avro/util"
)

func ReaderToMaps(
	rdr io.Reader,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var br io.Reader = bufio.NewReader(rdr)
		var dec *fc.Decoder = fc.NewDecoder(br)

		var buf map[string]any
		for {
			clear(buf)

			e := dec.Decode(&buf)
			if io.EOF == e {
				return
			}

			if !yield(buf, e) {
				return
			}
		}
	}
}

func StdinToMaps() iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin)
}

var MapsFromStdin IO[iter.Seq2[map[string]any, error]] = OfFn(StdinToMaps)
