package q

type Field string

func (f Field) Name() string {
	return string(f)
}
