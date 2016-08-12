# Machinery PG

This a prototype of using Postgres as broker and backend for [Machinery](https://github.com/RichardKnop/machinery).

For the broker part, it just polls the database each second so for efficiency, use AMQP or Redis brokers.
For the backend, it simply uses Postgres database for storing task details.

This prototype help me to understand how Machinery works and know how good is [GROM](https://github.com/jinzhu/gorm)

## Requirements

- Golang 1.6
- Postgres 9.4 (need `uuid` and `jsonb`)

## Licence

**MIT**

## Contributing

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Ensure specs and Rubocop pass
5. Push to the branch (git push origin my-new-feature)
6. Create new Pull Request
