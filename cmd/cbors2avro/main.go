package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-cbors2avro/util"

	eh "github.com/takanoriyanagitani/go-cbors2avro/avro/enc/hamba"
	da "github.com/takanoriyanagitani/go-cbors2avro/cbor/dec/amacker"
)

var GetEnvVarByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var schemaFilename IO[string] = GetEnvVarByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)

		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var cborMaps IO[iter.Seq2[map[string]any, error]] = da.MapsFromStdin

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	cborMaps,
	Lift(func(
		o iter.Seq2[map[string]any, error],
	) (iter.Seq2[map[string]any, error], error) {
		return eh.MapsToMaps(o), nil
	}),
)

var mapd2avro IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return mapd2avro(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
