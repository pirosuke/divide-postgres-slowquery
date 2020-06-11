# divide-postgres-slowquery
Create sql files from PostgreSQL slow query log.

## Usage

```
divide_pg_slowquery [flags]
  -c string
        Config File Path
  -f string
        Slow Query Log File Path
  -o string
        SQL Output Dir Path (default ".")
```

## Config File Example

```json
{
    "pattern_start": "^< ([^\\]]*) >LOG:  duration: (.*) ms  execute <unnamed>: (.*)",
    "pattern_end": "(.*)DETAIL:  parameters: (.*)"
}
```
