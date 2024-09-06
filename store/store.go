package store

type Store[T any] interface {
	Get(key string) (T, bool)
	Put(key string, payload T)

	// Begins either a readonly or writable transaction. Useful if you want to atomically get a
	// value from the store and update it. Example usage to duplicate the key "foo" to "bar":
	//
	// tx := store.BeginTx(false)
	// if val, ok := tx.Get("foo"); ok {
	//		tx.Put("bar", val)
	// }
	//
	// err := tx.Commit()
	// if err != nil {
	//		// Handle this at the application layer
	// }
	BeginTx(readonly bool) Tx[T]
}

type Tx[T any] interface {
	Get(key string) (T, bool)
	Put(key string, payload T)
	Commit() error
	Rollback() error
}
