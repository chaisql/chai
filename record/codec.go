package record

// A Decoder decodes encoded records.
type Decoder interface {
	Decode([]byte) (Record, error)
}

// An Encoder encodes records.
type Encoder interface {
	Encode(Record) ([]byte, error)
}

// A Codec can encode and decode records.
type Codec interface {
	Encoder
	Decoder
}

type codec struct{}

func NewCodec() Codec {
	return new(codec)
}

func (c *codec) Decode(data []byte) (Record, error) {
	return EncodedRecord(data), nil
}

func (c *codec) Encode(r Record) ([]byte, error) {
	return Encode(r)
}
