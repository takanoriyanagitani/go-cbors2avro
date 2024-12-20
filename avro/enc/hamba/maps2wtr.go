package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	ca "github.com/takanoriyanagitani/go-cbors2avro"
	. "github.com/takanoriyanagitani/go-cbors2avro/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(s, w, opts...)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		e = enc.Encode(row)
		if nil != e {
			return e
		}

		e = enc.Flush()
		if nil != e {
			return e
		}
	}
	return enc.Flush()
}

var codecMap map[ca.Codec]ho.CodecName = map[ca.Codec]ho.CodecName{
	ca.CodecNull:    ho.Null,
	ca.CodecDeflate: ho.Deflate,
	ca.CodecSnappy:  ho.Snappy,
	ca.CodecZstd:    ho.ZStandard,
}

func GetValOrAlt[K comparable, V any](m map[K]V, key K, alt V) V {
	val, found := m[key]
	switch found {
	case true:
		return val
	default:
		return alt
	}
}

func MapToGetterAlt[K comparable, V any](
	alt V,
	m map[K]V,
) func(K) V {
	return func(k K) V {
		return GetValOrAlt(m, k, alt)
	}
}

var CodecConverter func(ca.Codec) ho.CodecName = MapToGetterAlt(
	ho.Null,
	codecMap,
)

func ConfigToOpts(cfg ca.OutputConfig) []ho.EncoderFunc {
	var blockLen int = cfg.BlockLength
	var codec ho.CodecName = CodecConverter(cfg.Codec)
	return []ho.EncoderFunc{
		ho.WithBlockLength(blockLen),
		ho.WithCodec(codec),
	}
}

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	schema string,
	cfg ca.OutputConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapsToWriterHamba(
		ctx,
		m,
		w,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
	cfg ca.OutputConfig,
) error {
	return MapsToWriter(
		ctx,
		m,
		os.Stdout,
		schema,
		cfg,
	)
}

func ConfigToSchemaToMapsToStdout(
	cfg ca.OutputConfig,
) func(schema string) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(schema string) func(iter.Seq2[map[string]any, error]) IO[Void] {
		return func(m iter.Seq2[map[string]any, error]) IO[Void] {
			return func(ctx context.Context) (Void, error) {
				return Empty, MapsToStdout(
					ctx,
					m,
					schema,
					cfg,
				)
			}
		}
	}
}

var SchemaToMapsToStdoutDefault func(
	schema string,
) func(
	iter.Seq2[map[string]any, error],
) IO[Void] = ConfigToSchemaToMapsToStdout(ca.OutputConfigDefault)
