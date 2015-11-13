# cache
Cache is the bottom layer to redis.

###To use this
```
go get github.com/LeeQY/cache
```

###The code
```Go
import (
	"github.com/LeeQY/cache"
)

// The redis address.
address := "localhost:6379"

// Open the connection pool.
// Pass in the address, max connections, idle duration.
Open(address, 1, 5*time.Second)

// Finally, close the connection pool.
defer Close()

// Check whether the key is existed.
exist, ok := cache.CheckCache(&key)

// Set a cache.
ok = cache.SetCache(&key, &value)

// Get a cache.
v, ok = cache.GetCache(&key)

// Delete a cache.
ok = cache.DelCache(&key)

// Multiply set keys in one command.
ok = cache.MSetCache(&keys, &values)

// Multiply get values in one command.
mv, err = cache.MGetCache(&keys)

// Multiply delete keys in one command.
err = cache.MDelCache(&keys);

// Set a cache with expiration
err = cache.SetCacheEX(&key, &value, ex)

// Update keys with expirations
err = cache.UpdateExpire(&keys, &exs)
```