package cache

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

var (
	//	address := "localhost:6379"
	address = "192.168.1.103:6379"
)

func BenchmarkSetCache(b *testing.B) {
	Open(address, 1, 5*time.Second)
	defer Close()

	b.ResetTimer()
	count := 0
	for i := 0; i < b.N; i++ {
		k := strconv.Itoa(i)
		SetCache(&k, &k)
		count++
	}
	b.StopTimer()

	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = strconv.Itoa(i)
	}

	err := MDelCache(&keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkMSetCache(b *testing.B) {
	Open(address, 1, 5*time.Second)
	defer Close()

	p := 19900000000
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = strconv.Itoa(p)
		p++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := MSetCache(&keys, &keys)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	err := MDelCache(&keys)
	if err != nil {
		b.Error(err)
	}
}

func TestCache(t *testing.T) {
	Open(address, 1, 5*time.Second)
	defer Close()

	var err error

	prefix := "test"
	key := "test:1234567"
	value := "test"

	if exist, err := CheckCache(&key); err != nil {
		t.Fatal(err)
	} else if exist {
		t.Error("Key should not be existed.")
	}

	if err := SetCache(&key, &value); err != nil {
		t.Fatal(err)
	}

	if v, err := GetCache(&key); err != nil {
		t.Fatal(err)
	} else {
		if *v != value {
			t.Error("The get value is not the same with the set value.")
		}
	}

	if err := DelCache(&key); err != nil {
		t.Fatal(err)
	}

	if exist, err := CheckCache(&key); err != nil {
		t.Fatal(err)
	} else if exist {
		t.Error(err)
	}

	//test the multi-methods
	keys := make([]string, 5)
	values := make([]string, 5)
	for i := 0; i < 5; i++ {
		keys[i] = fmt.Sprintf("%s:%s", prefix, strconv.Itoa(i))
		values[i] = strconv.Itoa(i)
	}

	if err := MSetCache(&keys, &values); err != nil {
		t.Fatal(err)
	}

	var mv *[]string
	mv, err = MGetCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(values, *mv) {
		t.Fatal("MGet result failed.")
	}

	if err := MDelCache(&keys); err != nil {
		t.Fatal(err)
	}

	mv, err = MGetCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if empty := make([]string, 5); !reflect.DeepEqual(empty, *mv) {
		t.Fatal("MDel result failed.")
	}

	//test time exp
	var ex uint64 = 2592000

	if err := SetCacheEX(&key, &value, ex); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	if ttl, err := GetCacheTTL(&key); err != nil {
		t.Error(err)
	} else if ttl != 2591999 {
		t.Error("ttl is wrong.")
	}

	keysn := []string{key}
	exs := []uint64{ex}
	if err := UpdateExpiration(&keysn, &exs); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	if ttl, err := GetCacheTTL(&key); err != nil {
		t.Error(err)
	} else if ttl != 2591999 {
		t.Error("ttl is wrong.")
	}

	if err := DelCache(&key); err != nil {
		t.Error(err)
	}

	// test nil return
	if k, err := GetCache(&key); err != nil {
		t.Error(err)
	} else if k != nil {
		t.Error("k should be nil.")
	}
}
