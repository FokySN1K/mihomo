package cachefile

import (
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/bbolt"
)

var bucketReputation = []byte("reputation")

// ReputationMap returns every persisted reputation entry. The values stay
// opaque to the cache: the reputation store owns their encoding, so the format
// can change without touching the cache file.
func (c *CacheFile) ReputationMap() map[string][]byte {
	if c.DB == nil {
		return nil
	}

	mapping := map[string][]byte{}
	err := c.DB.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket(bucketReputation)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			mapping[string(k)] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		log.Warnln("[CacheFile] read reputation from %s failed: %s", c.DB.Path(), err.Error())
		return nil
	}
	return mapping
}

// SetReputations writes the changed entries and deletes the evicted ones in
// one transaction, so a flush never leaves the store half updated.
func (c *CacheFile) SetReputations(entries map[string][]byte, deletes []string) {
	if c.DB == nil || (len(entries) == 0 && len(deletes) == 0) {
		return
	}

	err := c.DB.Batch(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists(bucketReputation)
		if err != nil {
			return err
		}
		for _, name := range deletes {
			if err := bucket.Delete([]byte(name)); err != nil {
				return err
			}
		}
		for name, data := range entries {
			if err := bucket.Put([]byte(name), data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Warnln("[CacheFile] write reputation to %s failed: %s", c.DB.Path(), err.Error())
	}
}
