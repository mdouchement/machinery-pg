# Machinery PG

[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/mdouchement/machinery-pg)


This a prototype of using Postgres as broker and backend for [Machinery](https://github.com/RichardKnop/machinery).


Broker: it just polls the database each second so for efficiency you need to use a message queue (e.g. AMQP).

Backend: it simply uses Postgres database for storing task details.


This prototype help me to understand how Machinery works and to know how good is [GORM](https://github.com/jinzhu/gorm).

## Requirements

- Golang >= 1.6
- Postgres >= 9.4 (need `uuid` and `jsonb`)

## Usage

Take a look to the `example` folder.

SQL requests' logging can be enabled in `database.go`.

## Licence

**MIT**

## Contributing

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
5. Push to the branch (git push origin my-new-feature)
6. Create new Pull Request
