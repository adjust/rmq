package rmq

import (
	"errors"
	"strings"
	"sync"
	"time"
)

//TestRedisClient is a mock for redis
type TestRedisClient struct {
	store sync.Map
	ttl   sync.Map
}

var lock sync.Mutex

//NewTestRedisClient returns a NewTestRedisClient
func NewTestRedisClient() *TestRedisClient {
	return &TestRedisClient{}
}

// Set sets key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful SET operation.
func (client *TestRedisClient) Set(key string, value string, expiration time.Duration) error {

	lock.Lock()
	defer lock.Unlock()

	client.store.Store(key, value)
	//Delete any previous time to live associated with the key
	client.ttl.Delete(key)

	//0.0 expiration means that the value won't expire
	if expiration.Seconds() != 0.0 {
		//Store the unix time at which we should delete this
		client.ttl.Store(key, time.Now().Add(expiration).Unix())
	}

	return nil
}

// Get the value of key.
// If the key does not exist or isn't a string
// the special value nil is returned.
func (client *TestRedisClient) Get(key string) (string, error) {

	value, found := client.store.Load(key)

	if found {
		if stringValue, casted := value.(string); casted {
			return stringValue, nil
		}
	}

	return "nil", nil
}

//Del removes the specified key. A key is ignored if it does not exist.
func (client *TestRedisClient) Del(key string) (affected int64, err error) {

	_, found := client.store.Load(key)
	client.store.Delete(key)
	client.ttl.Delete(key)

	if found {
		return 1, nil
	}
	return 0, nil

}

// TTL returns the remaining time to live of a key that has a timeout.
// This introspection capability allows a Redis client to check how many seconds a given key will continue to be part of the dataset.
// In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist but has no associated expire.
// Starting with Redis 2.8 the return value in case of error changed:
// The command returns -2 if the key does not exist.
// The command returns -1 if the key exists but has no associated expire.
func (client *TestRedisClient) TTL(key string) (ttl time.Duration, err error) {

	//Lookup the expiration map
	expiration, found := client.ttl.Load(key)

	//Found an expiration time
	if found {

		//It was there, but it expired; removing it now
		if expiration.(int64) < time.Now().Unix() {
			client.ttl.Delete(key)
			return -2, nil
		}

		ttl = time.Duration(expiration.(int64) - time.Now().Unix())
		return ttl, nil
	}

	//Lookup the store in case this key exists but don't have an expiration
	//date
	_, found = client.store.Load(key)

	//The key was in store but didn't have an expiration associated
	//to it.
	if found {
		return -1, nil
	}

	return -2, nil
}

// LPush inserts the specified value at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
// When key holds a value that is not a list, an error is returned.
// It is possible to push multiple elements using a single command call just specifying multiple arguments
// at the end of the command. Elements are inserted one after the other to the head of the list,
// from the leftmost element to the rightmost element.
func (client *TestRedisClient) LPush(key string, value ...string) (total int64, err error) {

	lock.Lock()
	defer lock.Unlock()

	list, err := client.findList(key)

	if err != nil {
		return 0, nil
	}

	client.storeList(key, append(value, list...))
	return int64(len(list)) + 1, nil
}

//LLen returns the length of the list stored at key.
//If key does not exist, it is interpreted as an empty list and 0 is returned.
//An error is returned when the value stored at key is not a list.
func (client *TestRedisClient) LLen(key string) (affected int64, err error) {
	list, err := client.findList(key)

	if err != nil {
		return 0, nil
	}
	return int64(len(list)), nil
}

// LRem removes the first count occurrences of elements equal to
// value from the list stored at key. The count argument influences
// the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value. For example,
// LREM list -2 "hello" will remove the last two occurrences of "hello" in
// the list stored at list. Note that non-existing keys are treated like empty
// lists, so when key does not exist, the command will always return 0.
func (client *TestRedisClient) LRem(key string, count int64, value string) (affected int64, err error) {

	lock.Lock()
	defer lock.Unlock()

	list, err := client.findList(key)

	//Wasn't a list, or is empty
	if err != nil || len(list) == 0 {
		return 0, nil
	}

	//Create a list that have the capacity to store
	//the old one
	//This will be much more performant in case of
	//very long list
	newList := make([]string, 0, len(list))

	if err != nil {
		return 0, nil
	}

	//left to right removal of count elements
	if count >= 0 {

		//All the elements are to be removed.
		//Set count to max possible elements
		if count == 0 {
			count = int64(len(list))
		}
		//left to right traversal
		for index := 0; index < len(list); index++ {

			//isn't what we look for or we found enough element already
			if strings.Compare(list[index], value) != 0 || affected > count {
				newList = append(newList, list[index])
			} else {
				affected++
			}
		}
		//right to left removal of count elements
	} else if count < 0 {

		//right to left traversal
		for index := len(list) - 1; index >= 0; index-- {

			//isn't what we look for or we found enough element already
			if strings.Compare(list[index], value) != 0 || affected > count {
				//prepend instead of append to keep the order
				newList = append([]string{list[index]}, newList...)
			} else {
				affected++
			}
		}
	}

	//store the updated list
	client.storeList(key, newList)

	return affected, nil
}

// LTrim trims an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head),
// 1 the next element and so on. For example: LTRIM foobar 0 2 will modify the list stored
// at foobar so that only the first three elements of the list will remain.
// start and end can also be negative numbers indicating offsets from the end of the list,
// where -1 is the last element of the list, -2 the penultimate element and so on.
// Out of range indexes will not produce an error: if start is larger than the end of the list,
// or start > end, the result will be an empty list (which causes key to be removed).
// If end is larger than the end of the list, Redis will treat it like the last element of the list
func (client *TestRedisClient) LTrim(key string, start, stop int64) error {

	lock.Lock()
	defer lock.Unlock()

	list, err := client.findList(key)

	//Wasn't a list, or is empty
	if err != nil || len(list) == 0 {
		return nil
	}

	if start < 0 {
		start += int64(len(list))
	}
	if stop < 0 {
		stop += int64(len(list))
	}

	//invalid values cause the remove of the key
	if start > stop {
		client.store.Delete(key)
		return nil
	}

	client.storeList(key, list[start:stop])
	return nil
}

// RPopLPush atomically returns and removes the last element (tail) of the list stored at source,
// and pushes the element at the first element (head) of the list stored at destination.
// For example: consider source holding the list a,b,c, and destination holding the list x,y,z.
// Executing RPOPLPUSH results in source holding a,b and destination holding c,x,y,z.
// If source does not exist, the value nil is returned and no operation is performed.
// If source and destination are the same, the operation is equivalent to removing the
// last element from the list and pushing it as first element of the list,
// so it can be considered as a list rotation command.
func (client *TestRedisClient) RPopLPush(source, destination string) (value string, err error) {

	lock.Lock()
	defer lock.Unlock()

	sourceList, sourceErr := client.findList(source)
	destList, destErr := client.findList(destination)

	//One of the two isn't a list
	if sourceErr != nil || destErr != nil {
		return "", ErrorNotFound
	}
	//we have nothing to move
	if len(sourceList) == 0 {
		return "", ErrorNotFound
	}

	//Remove the last element of source (tail)
	client.storeList(source, sourceList[0:len(sourceList)-1])
	//Put the last element of source (tail) and prepend it to dest
	client.storeList(destination, append([]string{sourceList[len(sourceList)-1]}, destList...))

	return sourceList[len(sourceList)-1], nil
}

// SAdd adds the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
// An error is returned when the value stored at key is not a set.
func (client *TestRedisClient) SAdd(key, value string) (total int64, err error) {

	lock.Lock()
	defer lock.Unlock()

	set, err := client.findSet(key)
	if err != nil {
		return 0, err
	}

	set[value] = struct{}{}
	client.storeSet(key, set)
	return int64(len(set)), nil
}

// SMembers returns all the members of the set value stored at key.
// This has the same effect as running SINTER with one argument key.
func (client *TestRedisClient) SMembers(key string) (members []string, err error) {
	set, err := client.findSet(key)
	if err != nil {
		return members, nil
	}

	members = make([]string, 0, len(set))
	for k := range set {
		members = append(members, k)
	}

	return members, nil
}

// SRem removes the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
// An error is returned when the value stored at key is not a set.
func (client *TestRedisClient) SRem(key, value string) (affected int64, err error) {

	lock.Lock()
	defer lock.Unlock()

	set, err := client.findSet(key)
	if err != nil || len(set) == 0 {
		return 0, nil
	}

	if _, found := set[value]; found != false {
		delete(set, value)
		return 1, nil
	}

	return 0, nil
}

// FlushDb delete all the keys of the currently selected DB. This command never fails.
func (client *TestRedisClient) FlushDb() error {
	client.store = *new(sync.Map)
	client.ttl = *new(sync.Map)
	return nil
}

//storeSet stores a set
func (client *TestRedisClient) storeSet(key string, set map[string]struct{}) {
	client.store.Store(key, set)
}

//findSet finds a set
func (client *TestRedisClient) findSet(key string) (map[string]struct{}, error) {
	//Lookup the store for the list
	storedValue, found := client.store.Load(key)
	if found {
		//list are stored as pointer to []string
		set, casted := storedValue.(map[string]struct{})

		if casted {
			return set, nil
		}

		return nil, errors.New("Stored value wasn't a set")
	}

	//return an empty set if not found
	return make(map[string]struct{}), nil
}

//storeList is an helper function so others don't have to deal with pointers
func (client *TestRedisClient) storeList(key string, list []string) {
	client.store.Store(key, &list)
}

//findList returns the list stored at key.
//if key doesn't exist, an empty list is returned
//an error is returned when the value at key isn't a list
func (client *TestRedisClient) findList(key string) ([]string, error) {
	//Lookup the store for the list
	storedValue, found := client.store.Load(key)
	if found {

		//list are stored as pointer to []string
		list, casted := storedValue.(*[]string)

		//Successful cass from interface{} to *[]string
		//Preprend the new key
		if casted {

			//This mock use sync.Map to be thread safe.
			//sync.Map only accepts interface{} as values and
			//in order to store an array as interface{}, you need
			//to use a pointer to it.
			//We could return the pointer instead of the value
			//and gain some performances here. Returning the pointer,
			//however, will open us up to race conditions.
			return *list, nil
		}

		return nil, errors.New("Stored value wasn't a list")
	}

	//return an empty list if not found
	return []string{}, nil
}
