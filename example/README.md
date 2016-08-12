# Example

First of all, you need to configure the `cnf` variable according to your Postgres installation

- Start the worker

```sh
go run machinery_pg.go -w
```

- Send the predefined task
```sh
go run machinery_pg.go -s
```

- Send a predefined tasks group
```sh
go run machinery_pg.go -sg
```
