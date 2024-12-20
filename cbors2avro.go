package cbors2avro

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

type OutputConfig struct {
	BlockLength int
	Codec
}

const CodecDefault Codec = CodecNull
const BlockLengthDefault int = 100

var OutputConfigDefault OutputConfig = OutputConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecDefault,
}
