package cache

import (
	"errors"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	pool *redis.Pool
)

// Open the redis pool. If already open, which means "pool" is not nil, do nothing.
func Open(addr string, max int, idle time.Duration) {
	if pool != nil {
		return
	}

	pool = &redis.Pool{
		MaxIdle:     max,
		IdleTimeout: idle,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// Close the pool. Usually used when application ended.
func Close() {
	pool.Close()
	pool = nil
}

// Get a connection from the pool.
func getConn() (*redis.Conn, error) {
	if pool == nil {
		return nil, errors.New("Pool is not created.")
	}

	//Get 5 times
	for i := 0; i < 5; i++ {
		if conn := pool.Get(); conn.Err() == nil {
			return &conn, nil
		} else {
			conn.Close()
			time.Sleep(2 * time.Second)
		}
	}

	return nil, errors.New("Can't get a connection from redis pool.")
}

// Get the cache string value for the key.
//
// Return: value and ok
func GetStringCache(key *string) (*string, error) {
	if key == nil {
		return nil, errors.New("Key can't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	if reply, err := redis.String((*conn).Do("GET", *key)); err != nil {
		if err == redis.ErrNil {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return &reply, nil
	}
}

// Get the cache bytes value for the key.
//
// Return: value and ok
func GetBytesCache(key *string) ([]byte, error) {
	if key == nil {
		return nil, errors.New("Key can't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	if reply, err := redis.Bytes((*conn).Do("GET", *key)); err != nil {
		if err == redis.ErrNil {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return reply, nil
	}
}

// Get a key's TTL.
//
// Return -1 if no exp, -2 if not existed.
func GetCacheTTL(key *string) (int64, error) {
	if key == nil {
		return 0, errors.New("Key can't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return 0, err
	}
	defer (*conn).Close()

	if reply, err := redis.Int64((*conn).Do("TTL", *key)); err != nil {
		if err == redis.ErrNil {
			return -2, nil
		} else {
			return 0, err
		}
	} else {
		return reply, nil
	}
}

// MGet the cache string values for the keys.
//
// Return: the values and ok. If the value for one is not exist, it will be nil"
func MGetStringCache(keys []string) ([]*string, error) {
	if keys == nil {
		return nil, errors.New("keys should not be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	kL := len(keys)
	if kL == 0 {
		return nil, nil
	}

	var keyN []interface{}
	for i := 0; i < kL; i++ {
		keyN = append(keyN, keys[i])
	}

	if reply, err := redis.Values((*conn).Do("MGET", keyN...)); err != nil {
		return nil, err
	} else if len(reply) != kL {
		return nil, errors.New("The length of reply is not equal to number of keys.")
	} else {
		result := make([]*string, kL)
		for i := 0; i < kL; i++ {
			if one, err := redis.String(reply[i], nil); err != nil {
				if err == redis.ErrNil {
					result[i] = nil
				} else {
					return nil, err
				}
			} else {
				result[i] = &one
			}
		}
		return result, nil
	}
}

// MGet the cache bytes values for the keys.
//
// Return: the values and ok. If the value for one is not exist, it will be nil
func MGetBytesCache(keys []string) ([][]byte, error) {
	if keys == nil {
		return nil, errors.New("keys should not be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	kL := len(keys)

	var keyN []interface{}
	for i := 0; i < kL; i++ {
		keyN = append(keyN, keys[i])
	}

	if reply, err := redis.Values((*conn).Do("MGET", keyN...)); err != nil {
		return nil, err
	} else if len(reply) != kL {
		return nil, errors.New("The reply length isn't equal to the number of keys.")
	} else {
		result := make([][]byte, kL)
		for i := 0; i < kL; i++ {
			if one, err := redis.Bytes(reply[i], nil); err != nil {
				if err == redis.ErrNil {
					result[i] = nil
				} else {
					return nil, err
				}
			} else {
				result[i] = one
			}
		}
		return result, nil
	}
}

// MSet the cache string values for the keys.
//
// Return: error
func MSetStringCache(keys, values []string) error {
	if keys == nil {
		return errors.New("keys should not be nil.")
	}

	if values == nil {
		return errors.New("values should not be nil.")
	}

	kL := len(keys)
	if kL != len(values) {
		return errors.New("The length of keys and values are not the same.")
	}

	if kL == 0 {
		return nil
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	var v []interface{}
	for i := 0; i < kL; i++ {
		v = append(v, keys[i], values[i])
	}
	(*conn).Do("MSET", v...)

	return nil
}

// MSet the cache bytes values for the keys. Items for values can't be nil.
//
// Return: error
func MSetBytesCache(keys []string, values [][]byte) error {
	if keys == nil {
		return errors.New("keys should not be nil.")
	}

	if values == nil {
		return errors.New("values should not be nil.")
	}

	kL := len(keys)
	if kL != len(values) {
		return errors.New("The length of keys and values are not equal.")
	}

	if kL == 0 {
		return nil
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	var v []interface{}
	for i := 0; i < kL; i++ {
		if values[i] == nil {
			return errors.New("Value item can't be nil.")
		}
		v = append(v, keys[i], values[i])
	}
	(*conn).Do("MSET", v...)

	return nil
}

// Update a key's expiration.
//
// Return: error
func UpdateExpiration(k []string, ex []uint64) error {
	if k == nil {
		return errors.New("The keys should't be nil.")
	}

	if ex == nil {
		return errors.New("The ex should't be nil.")
	}

	kL := len(k)
	if kL != len(ex) {
		return errors.New("The length for k and ex are not the same.")
	}

	if kL == 0 {
		return nil
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	(*conn).Send("MULTI")
	for i := 0; i < kL; i++ {
		(*conn).Send("EXPIRE", k[i], ex[i])
	}
	(*conn).Do("EXEC")

	return nil
}

// Set a string cache with expiration in seconds.
//
// Return: error
func SetStringCacheEX(key, value *string, ex uint64) error {
	if key == nil {
		return errors.New("The key should't be nil.")
	}

	if value == nil {
		return errors.New("The value should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if _, err = (*conn).Do("SETEX", *key, ex, *value); err != nil {
		return err
	} else {
		return nil
	}
}

// Set a bytes cache with expiration in seconds.
//
// Return: error
func SetBytesCacheEX(key *string, value []byte, ex uint64) error {
	if key == nil {
		return errors.New("The key should't be nil.")
	}

	if value == nil {
		return errors.New("The value should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if _, err = (*conn).Do("SETEX", *key, ex, value); err != nil {
		return err
	} else {
		return nil
	}
}

// Set a string cache.
//
// Return: error
func SetStringCache(key, value *string) error {
	if key == nil {
		return errors.New("The key should't be nil.")
	}

	if value == nil {
		return errors.New("The value should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if _, err = (*conn).Do("SET", *key, *value); err != nil {
		return err
	} else {
		return nil
	}
}

// Set a bytes cache.
//
// Return: error
func SetBytesCache(key *string, value []byte) error {
	if key == nil {
		return errors.New("The key should't be nil.")
	}

	if value == nil {
		return errors.New("The value should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if _, err = (*conn).Do("SET", *key, value); err != nil {
		return err
	} else {
		return nil
	}
}

// Del the key
//
// Return: error
func DelCache(key *string) error {
	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if key == nil {
		return errors.New("key should not be nil.")
	}

	if err := (*conn).Send("DEL", *key); err != nil {
		return err
	}
	return nil
}

// MDel the keys
//
// Return: error
func MDelCache(keys []string) error {
	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if keys == nil {
		return errors.New("keys should not be nil.")
	} else if len(keys) == 0 {
		return nil
	}

	(*conn).Send("MULTI")
	for i := 0; i < len(keys); i++ {
		(*conn).Send("DEL", keys[i])
	}
	(*conn).Do("EXEC")

	return nil
}

// Check whether a key is existed.
func CheckCache(key *string) (bool, error) {
	if key == nil {
		return false, errors.New("The key should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return false, err
	}
	defer (*conn).Close()

	if e, err := redis.Int((*conn).Do("EXISTS", *key)); err != nil {
		return false, err
	} else {
		return e == 1, nil
	}
}

// offset From where to scan.
// prefix The prefix string to scan.
//
// Return: (offset, keys, error), if the offset is 0, means the scan is over.
func ListKeys(offset int64, prefix *string) (int64, []string, error) {
	if prefix == nil {
		return 0, nil, errors.New("Prefix can't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return 0, nil, err
	}
	defer (*conn).Close()

	var v []interface{}
	v, err = redis.Values((*conn).Do("SCAN", offset, *prefix))
	if err != nil {
		return 0, nil, err
	}

	var newOffset int64
	newOffsetS := string(v[0].([]byte))
	newOffset, err = strconv.ParseInt(newOffsetS, 10, 64)
	if err != nil {
		return 0, nil, err
	}

	keys := make([]string, 0)
	sv, _ := redis.Values(v[1], nil)
	for _, value := range sv {
		key := string(value.([]byte))
		keys = append(keys, key)
	}

	return newOffset, keys, nil
}
