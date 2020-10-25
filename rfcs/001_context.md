- Feature name: `Cancelable operations and use of Context`
- Date: 2020-10-10
- Author: Asdine El Hrychy
- Related discussions: [Issue #224](https://github.com/genjidb/genji/issues/224), [PR #231](https://github.com/genjidb/genji/pull/231)

## General approach

I would like to avoid context pollution as much as possible, but without restricting user flexibility. It is important to be able to control and cancel operations but I also believe that there are times where we don't care about that.
I want Genji to be simple to use and intuitive, and for that, I prefer to expand a bit more the surface API, rather than making the existing ones more complete and complex.
The purpose of this RFC is not to describe in detail how each operation should be canceled but rather which APIs should expect a context parameter.

## Genji package and database lifecycle

### Open and New

Opening a database relies on IO and should be cancelable, but after that operation is completed the database handle itself is long-lived and should not be cancelable, but closable with the `Close()` method.
However, most of the time, we don't care about how long it takes to open a database.

We currently have two ways of opening a database:

- `genji.Open`
- `genji.New`

`Open` is just a specialized way of opening a database which calls `New`. It is by nature incomplete:

- It only allows opening an in-memory database or a Bolt database
- It doesn't take any option

```go
db, err := genji.Open("foo.db")
if err != nil {
    return err
}
defer db.Close()
```

It is very simple to write and to use, it must remain as-is.

If users want more power, they must use the `genji.New` function.

Opening a database can take time, even for Bolt or Badger, because accessing disks is unbounded (slow disks, NFS, etc.). It makes sense to have this a cancelable operation.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
...

// ctx here is only used to control how long the engine creation can take
// as engines might wait for IO
ng := someengine.New(ctx, ...)
// but not all of them
ng := memoryengine.New()

// at startup, Genji performs some operations on the engine, this should be cancelable
db, err := genji.New(ctx, ng)
```

### genji.DB methods

By default, database methods must not expect a context. This avoids context pollution when cancelation is not necessary.
However, when it is necessary, a new handle can be created to use a specific context, using a new method named `WithContext`.

```go
// non-cancelable queries
db.Exec("INSERT INTO foo(a) VALUES (1)")
db.Query("SELECT 1")
db.QueryDocument("SELECT 1")

ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)

// WithContext returns a cancelable database handle
db.WithContext(ctx).Exec("INSERT INTO foo (a) VALUES (1)")

dbx := db.WithContext(ctx)

// any called dbx method must return an error if the context is canceled.
dbx.Query("SELECT 1")
dbx.Begin(true)
```

## Engines

Since almost every engine action is done from within a transaction, cancelation must be controlled by the context passed to the `Begin` method. Any action done on a canceled transaction must return an error.

In addition, context cancellation or some unrecoverable I/O error may occur when iterating over store with `engine.Iterator`. In such case iterator is invalidated, `Valid()` method returns false and the new `Err()` will return non-nil error.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)

// only the Begin method takes the context.
tx, err := ng.Begin(ctx, true)

// Transaction methods don't need to take a context.
st, err := tx.GetStore([]byte("foo"))
err = st.Put([]byte("a"), []byte("b"))

it := st.NewIterator(engine.IteratorConfig{})
defer it.Close()
for it.Seek(nil); it.Valid(); it.Next() {
	// …
}
if err := it.Err(); err != nil {
	return err
}
```

### Packages that use an engine

Since the context is passed only to `engine.Engine#Begin`, only the functions that explicitly need to open a transaction must expect a context themselves.

```go
package database

func (db *Database) Begin(ctx context.Context, writable bool) (*Transaction, error)
func (db *Database) BeginTx(ctx context.Context, writable bool) (*Transaction, error)
```

Any function using the returned transaction must not expect a context in their signature.

However, there might be some exceptions: If a function performs a long-running operation or relies on IO itself, it must expect a context.

### Other packages

If a function doesn't rely on IO, it must not expect a context.
