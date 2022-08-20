# Zap log hook for [Bun](https://bun.uptrace.dev/) ORM

## Install

`go get github.com/alc6/zapbun`

## Usage

```go
hook := NewQueryHook(logger,
    WithEnabled(false), // with hook log enabled true/false 
    WithVerbose(false), // verbose mode true/false
    WithLevels(queryLevel, errorLevel), // using levels from zapcore.Level
    WithDuration(false), // log the duration true/false
    WithDurationPrecision(time.Millisecond), // usually time.Millisecond/time.Microsecond
)

db.AddQueryHook(hook)
```