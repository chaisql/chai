package database

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/tree"
	"github.com/genjidb/genji/types"
)

var sequenceTableInfo = &TableInfo{
	TableName:      SequenceTableName,
	StoreNamespace: SequenceTableNamespace,
	FieldConstraints: MustNewFieldConstraints(
		&FieldConstraint{
			Position:  0,
			Field:     "name",
			Type:      types.TextValue,
			IsNotNull: true,
		},
		&FieldConstraint{
			Position: 1,
			Field:    "seq",
			Type:     types.IntegerValue,
		},
	),
	TableConstraints: []*TableConstraint{
		{
			Name: SequenceTableName + "_pk",
			Paths: []document.Path{
				document.NewPath("name"),
			},
			PrimaryKey: true,
		},
	},
}

// A Sequence manages a sequence of numbers.
// It is not thread safe.
type Sequence struct {
	Info *SequenceInfo

	CurrentValue *int64
	Cached       uint64
	Key          *tree.Key
}

// NewSequence creates a new or existing sequence. If currentValue is not nil
// next call to Next will increase the lease.
func NewSequence(info *SequenceInfo, currentValue *int64) Sequence {
	seq := Sequence{
		Info:         info,
		CurrentValue: currentValue,
	}

	// currentValue is not nil, the sequence already exists in the database
	// and the lease needs to be extended.
	if currentValue != nil {
		seq.Cached = seq.Info.Cache
	}

	return seq
}

func (s *Sequence) key() *tree.Key {
	if s.Key != nil {
		return s.Key
	}

	s.Key = tree.NewKey(types.NewTextValue(s.Info.Name))
	return s.Key
}

func (s *Sequence) Init(tx *Transaction, catalog *Catalog) error {
	tb, err := s.GetOrCreateTable(tx, catalog)
	if err != nil {
		return err
	}

	_, _, err = tb.Insert(document.NewFieldBuffer().Add("name", types.NewTextValue(s.Info.Name)))
	return err
}

func (s *Sequence) Drop(tx *Transaction, catalog *Catalog) error {
	tb, err := catalog.GetTable(tx, SequenceTableName)
	if err != nil {
		if errs.IsNotFoundError(err) {
			return nil
		}

		return err
	}

	k := s.key()

	return tb.Delete(k)
}

func (s *Sequence) Next(tx *Transaction, catalog *Catalog) (int64, error) {
	if !tx.Writable {
		return 0, errors.New("cannot increment sequence on read-only transaction")
	}

	var newValue int64
	if s.CurrentValue == nil {
		newValue = s.Info.Start
	} else {
		newValue = *s.CurrentValue + s.Info.IncrementBy
	}

	if newValue < s.Info.Min {
		if !s.Info.Cycle {
			return 0, fmt.Errorf("reached minimum value of sequence %s", s.Info.Name)
		}

		newValue = s.Info.Max
	}
	if newValue > s.Info.Max {
		if !s.Info.Cycle {
			return 0, fmt.Errorf("reached maximum value of sequence %s", s.Info.Name)
		}

		newValue = s.Info.Min
	}

	var newLease int64

	s.Cached++

	// if the number of cached values is less than or equal to the cache,
	// we don't increase the lease.
	if s.CurrentValue != nil && s.Cached <= s.Info.Cache {
		s.CurrentValue = &newValue
		return newValue, nil
	}

	// we need to reset the number of cached values to 1
	if s.CurrentValue != nil {
		s.Cached = 1
	}

	// calculate the new lease depending on the direction
	// of the sequence
	if s.Info.IncrementBy > 0 {
		newLease = newValue + int64(s.Info.Cache) - 1
		if newLease > s.Info.Max {
			newLease = s.Info.Max
		}
	} else {
		newLease = newValue - int64(s.Info.Cache) + 1
		if newLease < s.Info.Min {
			newLease = s.Info.Min
		}
	}

	// store the new lease
	err := s.SetLease(tx, catalog, s.Info.Name, newLease)
	if err != nil {
		return 0, err
	}

	s.CurrentValue = &newValue
	return newValue, nil
}

func (s *Sequence) SetLease(tx *Transaction, catalog *Catalog, name string, v int64) error {
	tb, err := s.GetOrCreateTable(tx, catalog)
	if err != nil {
		return err
	}

	k := s.key()

	_, err = tb.Replace(k,
		document.NewFieldBuffer().
			Add("name", types.NewTextValue(name)).
			Add("seq", types.NewIntegerValue(v)),
	)
	return err
}

func (s *Sequence) GetOrCreateTable(tx *Transaction, catalog *Catalog) (*Table, error) {
	tb, err := catalog.GetTable(tx, SequenceTableName)
	if err == nil || !errs.IsNotFoundError(err) {
		return tb, err
	}

	err = catalog.CreateTable(tx, SequenceTableName, sequenceTableInfo)
	if err != nil {
		return nil, err
	}

	return catalog.GetTable(tx, SequenceTableName)
}

func (s *Sequence) Type() string {
	return "sequence"
}

func (s *Sequence) Name() string {
	return s.Info.Name
}

func (s *Sequence) SetName(name string) {
	s.Info.Name = name
}

func (s *Sequence) GenerateBaseName() string {
	var sb strings.Builder
	sb.WriteString(s.Info.Owner.TableName)
	if len(s.Info.Owner.Paths) > 0 {
		sb.WriteString("_")
		sb.WriteString(s.Info.Owner.Paths.String())
	}
	sb.WriteString("_seq")
	return sb.String()
}

// Release the sequence by storing the actual current value to the sequence table.
// If the sequence has cache, the cached value is overwritten.
func (s *Sequence) Release(tx *Transaction, catalog *Catalog) error {
	if s.CurrentValue == nil {
		return nil
	}

	err := s.SetLease(tx, catalog, s.Info.Name, *s.CurrentValue)
	if err != nil {
		return err
	}

	s.Cached = s.Info.Cache
	return nil
}

func (s *Sequence) Clone() *Sequence {
	return &Sequence{
		Info:         s.Info.Clone(),
		CurrentValue: s.CurrentValue,
		Cached:       s.Cached,
	}
}
