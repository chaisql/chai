package query

type Field string

func (f Field) Name() string {
	return string(f)
}

type Table string

func (t Table) Name() string {
	return string(t)
}
