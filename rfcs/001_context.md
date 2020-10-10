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

Each of these must be cancelable independently, at any given time while the database handle is opened.
Because cancelation is handled independently from the database handle, it is very intuitive and doesn't require preparing a different handle to be able to cancel a query.

```go
db.Exec(ctx, "INSERT INTO foo(a) VALUES (1)")
db.Query(ctx, "SELECT 1")
db.QueryDocument(ctx, "SELECT 1")
```

Since a transaction is short-lived and atomic, contexts must be used to cancel the whole transaction by passing a context to `Begin`.
Any call to the transaction handle methods must return an error and rollback the transaction if the context is canceled.

```go
tx, _ := db.Begin(ctx, true)
defer tx.Rollback()

tx.Exec("INSERT INTO foo(a) VALUES (1)")
tx.Query("SELECT 1")
tx.QueryDocument("SELECT 1")

tx.Commit()
```

This also applies to `db.View` and `db.Update`:

```go
db.View(ctx, func(tx *Tx) error) error {
    tx.Exec("INSERT INTO foo(a) VALUES (1)")
    tx.Query("SELECT 1")
    tx.QueryDocument("SELECT 1")
})

db.Update(ctx, func(tx *Tx) error) error {
    tx.Exec("INSERT INTO foo(a) VALUES (1)")
    tx.Query("SELECT 1")
    tx.QueryDocument("SELECT 1")
})
```

### The case of Close

I don't think we should pass a context to `db.Close()`. Closing a database handle can take time, depending on the type of engine used and one might want to control that.
But most of the time this is not important, so I'd rather add a specialized method.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)

err = db.Shutdown(ctx)
```

## Engines

Since almost every engine action is done from within a transaction, cancelation must be controlled by the context passed to the `Begin` method. Any action done on a canceled transaction must return an error.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)

// only the Begin method takes the context.
tx, err := ng.Begin(ctx, true)

// Transaction methods don't need to take a context.
st, err := tx.GetStore([]byte("foo"))
err = st.Put([]byte("a"), []byte("b"))

it := st.NewIterator(engine.IteratorConfig{})
defer it.Close()
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
