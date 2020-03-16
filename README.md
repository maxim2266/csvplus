# csvplus

[![GoDoc](https://godoc.org/github.com/maxim2266/csvplus?status.svg)](https://pkg.go.dev/github.com/maxim2266/csvplus)
[![Go report](http://goreportcard.com/badge/maxim2266/csvplus)](http://goreportcard.com/report/maxim2266/csvplus)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-yellow.svg)](https://opensource.org/licenses/BSD-3-Clause)

Package `csvplus` extends the standard Go [encoding/csv](https://golang.org/pkg/encoding/csv/)
package with fluent interface, lazy stream processing operations, indices and joins.

The library is primarily designed for [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)-like processes.
It is mostly useful in places where the more advanced searching/joining capabilities of a fully-featured SQL
database are not required, but the same time the data transformations needed still include SQL-like operations.

##### License: BSD

### Examples

Simple sequential processing:
```Go
people := csvplus.FromFile("people.csv").SelectColumns("name", "surname", "id")

err := csvplus.Take(people).
	Filter(csvplus.Like(csvplus.Row{"name": "Amelia"})).
	Map(func(row csvplus.Row) csvplus.Row { row["name"] = "Julia"; return row }).
	ToCsvFile("out.csv", "name", "surname")

if err != nil {
	return err
}
```

More involved example:
```Go
customers := csvplus.FromFile("people.csv").SelectColumns("id", "name", "surname")
custIndex, err := csvplus.Take(customers).UniqueIndexOn("id")

if err != nil {
	return err
}

products := csvplus.FromFile("stock.csv").SelectColumns("prod_id", "product", "price")
prodIndex, err := csvplus.Take(products).UniqueIndexOn("prod_id")

if err != nil {
	return err
}

orders := csvplus.FromFile("orders.csv").SelectColumns("cust_id", "prod_id", "qty", "ts")
iter := csvplus.Take(orders).Join(custIndex, "cust_id").Join(prodIndex)

return iter(func(row csvplus.Row) error {
	// prints lines like:
	//	John Doe bought 38 oranges for £0.03 each on 2016-09-14T08:48:22+01:00
	_, e := fmt.Printf("%s %s bought %s %ss for £%s each on %s\n",
		row["name"], row["surname"], row["qty"], row["product"], row["price"], row["ts"])
	return e
})
```

### Design principles

The package functionality is based on the operations on the following entities:
- type `Row`
- type `DataSource`
- type `Index`

#### Type `Row`
`Row` represents one row from a `DataSource`. It is a map from column names
to the string values under those columns on the current row. The package expects a unique name
assigned to every column at source. Compared to using integer indices this provides more
convenience when complex transformations get applied to each row during processing.

#### type `DataSource`
Type `DataSource` represents any source of zero or more rows, like `.csv` file. This is a function
that when invoked feeds the given callback with the data from its source, one `Row` at a time.
The type also has a number of operations defined on it that provide for easy composition of the
operations on the `DataSource`, forming so called [fluent interface](https://en.wikipedia.org/wiki/Fluent_interface).
All these operations are 'lazy', i.e. they are not performed immediately, but instead each of them
returns a new `DataSource`.

There is also a number of convenience operations that actually invoke
the `DataSource` function to produce a specific type of output:
- `IndexOn` to build an index on the specified column(s);
- `UniqueIndexOn` to build a unique index on the specified column(s);
- `ToCsv` to serialise the `DataSource` to the given `io.Writer` in `.csv` format;
- `ToCsvFile` to store the `DataSource` in the specified file in `.csv` format;
- `ToJSON` to serialise the `DataSource` to the given `io.Writer` in JSON format;
- `ToJSONFile` to store the `DataSource` in the specified file in JSON format;
- `ToRows` to convert the `DataSource` to a slice of `Row`s.

#### Type `Index`
`Index` is a sorted collection of rows. The sorting is performed on the columns specified when the index
is created. Iteration over an index yields a sorted sequence of rows. An `Index` can be joined with
a `DataSource`. The type has operations for finding rows and creating sub-indices in O(log(n)) time.
Another useful operation is resolving duplicates. Building an index takes O(n*log(n)) time. It should
be noted that the `Index` building operation requires the entire dataset to be read into
the memory, so certain care should be taken when indexing huge datasets. An index can also be
stored to, or loaded from a disk file.

For more details see the [documentation](https://godoc.org/github.com/maxim2266/csvplus).

### Project status
The project is in a usable state usually called "beta". Tested on Linux Mint 18.3 using Go version 1.10.2.
