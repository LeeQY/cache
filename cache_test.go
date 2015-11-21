package cache

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

var (
	address = "localhost:6379"
	//	address = "192.168.1.103:6379"

	prefix = "test"
	key    = "test:1234567"
	value  = "test"
)

func BenchmarkSetCache(b *testing.B) {
	Open(address, 1, 5*time.Second)
	defer Close()

	b.ResetTimer()
	count := 0
	for i := 0; i < b.N; i++ {
		k := strconv.Itoa(i)
		SetStringCache(&k, &k)
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
		err := MSetStringCache(&keys, &keys)
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

	// test string handle
	if exist, err := CheckCache(&key); err != nil {
		t.Fatal(err)
	} else if exist {
		t.Error("Key should not be existed.")
	}

	if err := SetStringCache(&key, &value); err != nil {
		t.Fatal(err)
	}

	if v, err := GetStringCache(&key); err != nil {
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

	//test bytes cache
	bKey := "test:byteskey"
	bArray := []string{"ab", "cd", "ef"}
	var bytes []byte
	if bytes, err = json.Marshal(&bArray); err != nil {
		t.Error(err)
	}

	if err := SetBytesCache(&bKey, &bytes); err != nil {
		t.Error(err)
	}

	if bResult, err := GetBytesCache(&bKey); err != nil {
		t.Error(err)
	} else {
		if !reflect.DeepEqual(bytes, *bResult) {
			t.Fatal("Bytes result is not equal.")
		}
	}

	if err := DelCache(&bKey); err != nil {
		t.Error(err)
	}

	//test the multi-methods for string
	testMultipleString(t)

	//test multiple methods for bytes
	testMultipleBytes(t)

	//test time exp
	testTimeEXP(t)

	// test nil return
	if k, err := GetStringCache(&key); err != nil {
		t.Error(err)
	} else if k != nil {
		t.Error("k should be nil.")
	}
}

func testMultipleString(t *testing.T) {
	var err error

	keys := make([]string, 5)
	values := make([]string, 5)
	for i := 0; i < 5; i++ {
		keys[i] = fmt.Sprintf("%s:%s", prefix, strconv.Itoa(i))
		values[i] = strconv.Itoa(i)
	}

	if err := MSetStringCache(&keys, &values); err != nil {
		t.Fatal(err)
	}

	var mv *[]string
	mv, err = MGetStringCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(values, *mv) {
		t.Fatal("MGet result failed.")
	}

	if err := MDelCache(&keys); err != nil {
		t.Fatal(err)
	}

	mv, err = MGetStringCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if empty := make([]string, 5); !reflect.DeepEqual(empty, *mv) {
		t.Fatal("MDel result failed.")
	}
}

func testMultipleBytes(t *testing.T) {
	keys := make([]string, 5)
	bValues := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		keys[i] = fmt.Sprintf("%s:%s", prefix, strconv.Itoa(i))
		bValues[i] = []byte(strconv.Itoa(i))
	}

	if err := MSetBytesCache(&keys, &bValues); err != nil {
		t.Fatal(err)
	}

	var mbv *[][]byte
	var err error
	mbv, err = MGetBytesCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(bValues, *mbv) {
		t.Fatal("MGet result failed.")
	}

	if err := MDelCache(&keys); err != nil {
		t.Fatal(err)
	}

	mbv, err = MGetBytesCache(&keys)
	if err != nil {
		t.Fatal(err)
	}

	if empty := make([][]byte, 5); !reflect.DeepEqual(empty, *mbv) {
		t.Fatal("MDel result failed.")
	}
}

func testTimeEXP(t *testing.T) {
	var ex uint64 = 2592000

	if err := SetStringCacheEX(&key, &value, ex); err != nil {
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

	bValue := []byte("abc")
	if err := SetBytesCacheEX(&key, &bValue, ex); err != nil {
		t.Fatal(err)
	}

	if result, err := GetBytesCache(&key); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(*result, bValue) {
		t.Error("values are not the same.")
	}

	if err := DelCache(&key); err != nil {
		t.Error(err)
	}
}
