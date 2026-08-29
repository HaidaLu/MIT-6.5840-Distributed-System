package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	uniqueID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.uniqueID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		isAvailable := err == rpc.ErrNoKey || (err == rpc.OK && val == "")
		if isAvailable {

			err := lk.ck.Put(lk.lockname, lk.uniqueID, ver)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				val, _, err = lk.ck.Get(lk.lockname)
				if err == rpc.OK && val == lk.uniqueID {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		} else {
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		isAvailable := err == rpc.ErrNoKey || (err == rpc.OK && val == "")
		if !isAvailable {
			if val == lk.uniqueID {
				err := lk.ck.Put(lk.lockname, "", ver)
				if err != rpc.OK {
					// if put fails before we write the value, we need to retry
					val2, _, err2 := lk.ck.Get(lk.lockname)
					if err2 != rpc.OK {
						continue
					}
					if val2 == lk.uniqueID {
						continue
					}
				}
			}
			return
		} else {
			return
		}
	}

}
