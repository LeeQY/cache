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

// Set a string cache.
ok = cache.SetStringCache(&key, &value)

// Set a bytes cache.
ok = cache.SetBytesCache(&key, &bytes)

// Get a string cache.
v, ok = cache.GetStringCache(&key)

// Get a bytes cache.
v, ok = cache.GetBytesCache(&key)

// Delete a cache.
ok = cache.DelCache(&key)

// Multiply set string caches with keys in one command.
ok = cache.MSetStringCache(&keys, &values)

// Multiply set bytes caches with keys in one command.
ok = cache.MSetBytesCache(&keys, &bytes)

// Multiply get string values in one command.
mv, err = cache.MGetStringCache(&keys)

// Multiply get bytes values in one command.
mv, err = cache.MGetBytesCache(&keys)

// Multiply delete keys in one command.
err = cache.MDelCache(&keys);

// Set a string cache with expiration
err = cache.SetStringCacheEX(&key, &value, ex)

// Set a bytes cache with expiration
err = cache.SetBytesCacheEX(&key, &bytes, ex)

// Update keys with expirations
err = cache.UpdateExpire(&keys, &exs)
```