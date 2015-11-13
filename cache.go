package cache

import (
	"errors"
	"log"
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

	log.Println("Redis pool is open at ", addr)
}

// Close the pool. Usually used when application ended.
func Close() {
	pool.Close()
	pool = nil
}

// Get a connection from the pool.
func getConn() (*redis.Conn, error) {
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

// Get the cache value for the key.
//
// Return: value and ok
func GetCache(key *string) (*string, error) {
	if key == nil {
		return nil, errors.New("Key can't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	if reply, err := redis.String((*conn).Do("GET", *key)); err != nil {
		return nil, err
	} else {
		return &reply, nil
	}
}

// MGet the cache values for the keys.
//
// Return: the values and ok. If the value for one is not exist, then it will be ""
func MGetCache(keys *[]string) (*[]string, error) {
	if keys == nil {
		return nil, errors.New("keys should not be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	defer (*conn).Close()

	kL := len(*keys)

	var keyN []interface{}
	for i := 0; i < kL; i++ {
		keyN = append(keyN, (*keys)[i])
	}

	reply, _ := redis.Strings((*conn).Do("MGET", keyN...))
	result := make([]string, len(reply))
	for i := 0; i < len(reply); i++ {
		result[i] = reply[i]
	}
	return &result, nil
}

// MSet the cache values for the keys.
//
// Return: error
func MSetCache(keys *[]string, values *[]string) error {
	if keys == nil {
		return errors.New("keys should not be nil.")
	}

	if values == nil {
		return errors.New("values should not be nil.")
	}

	kL := len(*keys)
	if kL != len(*values) {
		return errors.New("The length of keys and values are not the same.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	var v []interface{}
	for i := 0; i < kL; i++ {
		v = append(v, (*keys)[i], (*values)[i])
	}
	(*conn).Do("MSET", v...)

	return nil
}

// Update a key's expiration.
//
// Return: error
func UpdateExpiration(k *[]string, ex *[]uint64) error {
	if k == nil {
		return errors.New("The keys should't be nil.")
	}

	if ex == nil {
		return errors.New("The ex should't be nil.")
	}

	kL := len(*k)
	if kL != len(*ex) {
		return errors.New("The length for k and ex are not the same.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	(*conn).Send("MULTI")
	for i := 0; i < kL; i++ {
		(*conn).Send("EXPIRE", (*k)[i], (*ex)[i])
	}
	(*conn).Do("EXEC")

	return nil
}

// Set a cache with expiration in seconds.
//
// Return: error
func SetCacheEX(key, value *string, ex uint64) error {
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

	_, err = (*conn).Do("SETEX", *key, ex, *value)

	if err != nil {
		return err
	}

	return nil
}

// Set a cache.
//
// Return: error
func SetCache(key, value *string) error {
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

	_, err = (*conn).Do("SET", *key, *value)

	if err != nil {
		return err
	}

	return nil
}

// Delete the cache with key.
//
// Return: error
func DelCache(key *string) error {
	if key == nil {
		return errors.New("The key should't be nil.")
	}

	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	var s int
	if s, err = redis.Int((*conn).Do("DEL", *key)); err != nil {
		return err
	} else {
		if s == 1 {
			return nil
		}
		return errors.New("Error to delete cache.")
	}
}

// MDel the keys
//
// Return: error
func MDelCache(keys *[]string) error {
	conn, err := getConn()
	if err != nil {
		return err
	}
	defer (*conn).Close()

	if keys == nil {
		return errors.New("keys should not be nil.")
	}

	kL := len(*keys)
	(*conn).Send("MULTI")
	for i := 0; i < kL; i++ {
		(*conn).Send("DEL", (*keys)[i])
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

	var e int
	if e, err = redis.Int((*conn).Do("EXISTS", *key)); err != nil {
		return false, err
	}

	return e == 1, nil
}

// offset From where to scan.
// prefix The prefix string to scan.
//
// Return: (offset, keys, error), if the offset is 0, means the scan is over.
func ListKeys(offset int64, prefix *string) (int64, *[]string, error) {
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

	return newOffset, &keys, nil
}
