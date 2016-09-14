#csvplus

Package `csvplus` extends the standard Go encoding/csv package with stream processing, fluent interface,
lazy evaluation, indices and joins.

### Examples

Simple sequential processing:
```Go
	source := csvplus.CsvFileDataSource("people.csv").SelectColumns("name", "surname", "id")

	err := csvplus.Take(source).
		Filter(csvplus.Like(csvplus.Row{"name": "Amelia"})).
		Map(func(row csvplus.Row) csvplus.Row { row["name"] = "Julia"; return row }).
		ToCsvFile("out.csv", "name", "surname")

	if err != nil {
		return err
	}
```

More involved example:
```Go
	customers, err := csvplus.Take(csvplus.CsvFileDataSource("people.csv").SelectColumns("id", "name", "surname")).IndexOn("id")

	if err != nil {
		return err
	}

	products, err := csvplus.Take(csvplus.CsvFileDataSource("stock.csv").SelectColumns("prod_id", "product", "price")).IndexOn("prod_id")

	if err != nil {
		return err
	}

	orders := csvplus.CsvFileDataSource("orders.csv").SelectColumns("order_id", "cust_id", "prod_id", "qty", "ts")

	return customers.
		Join(orders, "cust_id").
		Join(products).
		ForEach(func(row csvplus.Row) error {
			_, e := fmt.Printf("%s %s bought %s %ss for £%s each on %s\n",
				row["name"], row["surname"], row["qty"], row["product"], row["price"], row["ts"])
			// From my sample data prints lines like:
			//	John Doe bought 38 oranges for £0.03 each on 2016-09-14T08:48:22+01:00
			return e
		})
```


For more details see the [documentation](https://godoc.org/github.com/maxim2266/csvplus).


##### License: BSD
