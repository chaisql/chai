/*
Package genji implements a relational database on top of key-value stores.
Genji supports various engines that write data on-disk, like BoltDB, or in memory.

It provides a complete framework with multiple APIs that can be used to manipulate, manage, read and write data.

Genji supports schemaful and schemaless tables that can be manipulated using the table package, which is a low level functional API
or by using the query package which is a powerful SQL like query engine.

Tables can be mapped to Go structures without reflection: Genji relies on code generation to translate data to and from Go structures.
*/
package genji
