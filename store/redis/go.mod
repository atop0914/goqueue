module github.com/atop0914/goqueue/store/redis

go 1.25.6

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/atop0914/goqueue v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/atop0914/goqueue => ../..
