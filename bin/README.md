# Scripting Tools

## Compose
```bin/compose```
is a simplified version of docker-compose script which includes all yml files

## Build stack

```shell
bin/build
```

## Start node

bin/start `<role>`

```shell
bin/start <worker_cpu | worker_gpu | producer | verifier>
```

## Stop node
```shell
bin/stop producer
```

## Attach to logs
```shell
bin/logs producer
```

## Shutdown
```
bin/compose down -v
```