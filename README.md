#csvplus

[![GoDoc](https://godoc.org/github.com/maxim2266/csvplus?status.svg)](https://godoc.org/github.com/maxim2266/csvplus)
[![Go report](http://goreportcard.com/badge/maxim2266/csvplus)](http://goreportcard.com/report/maxim2266/csvplus)

Package `csvplus` extends the standard Go [encoding/csv](https://golang.org/pkg/encoding/csv/)
package with fluent interface, lazy stream operations, indices and joins.

### Examples

Simple sequential processing:
```Go
people := csvplus.CsvFileDataSource("people.csv").SelectColumns("name", "surname", "id")

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
customers, err := csvplus.Take(
	csvplus.CsvFileDataSource("people.csv").SelectColumns("id", "name", "surname")).
	UniqueIndexOn("id")

if err != nil {
	return err
}

products, err := csvplus.Take(
	csvplus.CsvFileDataSource("stock.csv").SelectColumns("prod_id", "product", "price")).
	UniqueIndexOn("prod_id")

if err != nil {
	return err
}

orders := csvplus.CsvFileDataSource("orders.csv").SelectColumns("cust_id", "prod_id", "qty", "ts")

return csvplus.Take(orders).
	Join(customers, "cust_id").
	Join(products).
	ForEach(func(row csvplus.Row) error {
		_, e := fmt.Printf("%s %s bought %s %ss for £%s each on %s\n",
			row["name"], row["surname"], row["qty"], row["product"], row["price"], row["ts"])
		// prints lines like:
		//	John Doe bought 38 oranges for £0.03 each on 2016-09-14T08:48:22+01:00
		return e
	})
```

### Design principles

The package functionality is based on the operations on the following entities:
- type Row
- interface DataSource
- type Table
- type Index

#### Type `Row`
`Row` represents one row from a `DataSource`. It is a map from column names
to the string values under those columns on the current row. The package expects a unique name
assigned to every column at source. Compared to using integer indices this provides more
convenience when complex transformations are applied to each row during processing.

#### Interface `DataSource`
Interface `DataSource` represents any source of one or more rows, like `.csv` file. The only defined
operation on `DataSource` is iteration over the rows. The iteration is performed via an implementation of
`ForEach` method which is expected to call its parameter function once per each row. The package contains
an implementation of the interface for `.csv` files.

#### Type `Table`
Type `Table` implements sequential operations on a given data source, as well as the `DataSource`
interface itself and other iterating methods. All sequential operations are 'lazy', i.e. they are not
invoked immediately, but instead they return a new table which, when iterated over, invokes
the particular operation. The operations can be chained using so called [fluent interface](https://en.wikipedia.org/wiki/Fluent_interface).
The actual iteration over a table only happens when any of the following methods is called:
- `ForEach`
- `IndexOn`
- `UniqueIndexOn`
- `ToCsvFile`
- `ToRows`

A `Table` can also be joined with an `Index`, and this operation is lazy.

#### Type `Index`
Index is a sorted collection of rows. The sorting is performed on the columns specified when the index
is created. Iteration over an index yields sorted sequence of rows. An `Index` can be joined with
a `Table`. The type has operations for finding rows and creating sub-indices in O(log(n)) time.
Another useful operation is resolving duplicates. Building an index takes O(n*log(n)) time, but before that
the entire data source gets read into the memory so certain care should be taken when indexing
huge datasets.

For more details see the [documentation](https://godoc.org/github.com/maxim2266/csvplus).

### Project status
The project is in a usable state usually called "beta". Tested on Linux Mint 18 (based on Ubuntu 16.04).
Go version 1.7.1.

##### License: BSD
