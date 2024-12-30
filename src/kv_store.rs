use std::alloc::{Allocator, Global, GlobalAlloc, Layout};
use std::borrow::BorrowMut;
use std::ffi::CString;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::ptr::{self, NonNull};
use std::slice::from_raw_parts;

type HashUnit = usize;

// A generic kv store
pub trait KVStore {
    // TODO: for now.. our key value store just
    // uses string keys and returns string values
    // .. i.e. HashMap<String, String>
    fn set(key: String, value: String);
    fn get(key: String) -> String;
}

// A reimplemenation of the redis hashtable in Rust
pub struct HashTable<K, V, S = RandomState> {
    buckets: NonNull<[RawBucket<K, V>]>,
    // we can have child level buckets
    num_buckets: usize,
    hash_builder: S,
}

impl<K, V, S> HashTable<K, V, S> {
    // create a new hashmap .. allocating some buckets
    pub fn new_with_n_buckets(n_buckets: usize) -> Self
    where
        S: Default,
    {
        // Here .. I can use Vec<RawBucket> code here to allocate the buffer
        // but I'm going to copy the code from vec instead :)
        // unwrap any allocation errors
        let (layout, _) = Layout::new::<RawBucket<K, V>>().repeat(n_buckets).unwrap();
        // manually allocate memory for the bucket
        let ptr = Global
            .allocate_zeroed(layout)
            .expect("Couldn't allocate memory");
        Self {
            buckets: unsafe {
                from_raw_parts(ptr.as_ptr() as *mut RawBucket<K, V>, n_buckets).into()
            },
            hash_builder: Default::default(),
            num_buckets: n_buckets,
        }
    }

    /// insert a key and a value into the hashmap
    pub fn insert(&mut self, key: K, value: V)
    where
        K: Hash + Eq,
        S: BuildHasher,
    {
        // TODO: this is not done .. boviously

        // first .. determine which bucket this lands in by hashing the value
        let hash = self.hash_builder.hash_one(&key);
        // the number the bucket lands into
        // .. Redis uses an "expToMask" .. but for now I'll just mod
        let num_bucket = (hash as usize) % self.buckets.len();
        // A bucket can be a bytes of zeroes
        let b = unsafe { self.buckets.as_ptr().as_mut().unwrap() };
        b[num_bucket].insert_item(key, value, hash);
    }
}
impl<K, V, S> Drop for HashTable<K, V, S> {
    fn drop(&mut self) {
        // FIXME: need to make sure to deallocate any chained buckets that have been allocated
        // as well
        let (layout, _) = Layout::new::<RawBucket<K, V>>()
            .repeat(self.buckets.len())
            .unwrap();
        // FIXME: we also need to manually deallocate the boxes in the raw buckets as well..
        unsafe {
            Global.deallocate(self.buckets.cast(), layout);
        }
    }
}

const BUCKET_SIZE: usize = 7;

// A "bucket" contains a static vector that stores a bucket item
pub struct RawBucket<K, V> {
    // the first byte of a raw bucket contains metadata information
    // .. rust doesn't have bit fields like Zig .. although it'd be nice if rust did have bit fields
    meta: u8,
    hashes: [u8; BUCKET_SIZE],
    bucket: [BucketItem<K, V>; BUCKET_SIZE],
}

// implement raw bucket functionality
impl<K, V> RawBucket<K, V> {
    /// Check if the bucket is a chained bucket by looking at the first byte of metadata
    pub fn chained(&self) -> bool {
        ((self.meta >> 7) & 1) != 0
    }
    pub fn flip_chained(&mut self) {
        self.meta ^= 1 << 7
    }

    /// Check if a position is filled
    pub fn is_pos_filled(&self, pos: usize) -> bool {
        self.meta & (1 << pos) != 0
    }

    // number of bucket positions
    pub fn n_bucket_pos(&self) -> usize {
        if self.chained() {
            BUCKET_SIZE - 1
        } else {
            BUCKET_SIZE
        }
    }

    /// check if the bucget is full
    pub fn is_bucket_full(&self) -> bool {
        // clear out the first bit of meta
        let presense = if self.chained() {
            self.meta & 0b00111111
        } else {
            self.meta & 0b01111111
        };
        presense == (1 << self.n_bucket_pos()) - 1
    }

    /// Get the bucket value at position.
    fn get_bucket_value(&self, i: usize) -> Option<*mut (K, V)> {
        if self.chained() && i == BUCKET_SIZE - 1 || !self.is_pos_filled(i) {
            None
        } else {
            // because of the previous if condition .. we know that this is a value
            // because the last item can only be a pointer to another bucket when self is chained
            // so this must point to a bucket value
            unsafe { Some(self.bucket[i].value) }
        }
    }

    /// get a pointer to the child bucket, returning none of there is no child bucket
    fn get_child_bucket(&self) -> Option<&mut Self> {
        if self.chained() {
            // position has to be filled if the bucket is chained
            debug_assert!(self.is_pos_filled(BUCKET_SIZE - 1));
            // assert an invariant that when a bucket is chained, the slot is also marked as filled
            unsafe {
                Some(
                    self.bucket[BUCKET_SIZE - 1]
                        .ptr
                        .as_mut()
                        .expect("Child bucket should not be null when the bucket is chained"),
                )
            }
        } else {
            None
        }
    }

    fn bucket_levels(&self) -> usize {
        let mut count = 1;
        let mut c = self;
        while let Some(c1) = c.get_child_bucket() {
            count += 1;
            c = c1;
        }
        count
    }

    /// Insert an item into the bucket, along with the highbits
    /// when teh map does not have the key present, None is returned.
    /// If a new child bucket was allocated, the bool is true
    pub fn insert_item(&mut self, key: K, value: V, hash: u64) -> (Option<V>, bool)
    where
        K: Eq,
    {
        let highbits_hash = (hash >> 56) as u8;
        let bucket = self.find_bucket_mut(highbits_hash, &key);
        match bucket {
            Some((bucket, pos)) => {
                // bucket was found
                let bucket_item = unsafe {
                    bucket.bucket[pos]
                        .value
                        .as_mut()
                        .expect("bucket value should not be null")
                };
                (Some(std::mem::replace(&mut bucket_item.1, value)), false)
            }
            None => {
                // store the item into the bucket
                let (bucket, free_pos, new_b) = self.find_free_pos();
                bucket.meta |= 1 << free_pos;
                bucket.hashes[free_pos] = (hash >> 56) as u8;
                bucket.bucket[free_pos] = BucketItem {
                    value: Box::into_raw(Box::new((key, value))),
                };
                (None, new_b)
            }
        }
    }

    pub fn get(&self, hash: u64, key: &K) -> Option<&V>
    where
        K: Eq,
    {
        let highbits_hash = (hash >> 56) as u8;
        self.find_bucket(highbits_hash, key)
            .and_then(|(b, s)| b.get_bucket_value(s))
            .map(|p| unsafe { &p.as_ref().expect("Ref should not be null").1 })
    }

    /// Finds a bucket item that matches a given key
    /// If the bucket (or buckets) are full and the key doesn't exist,
    /// returns None
    fn find_bucket(&self, highbits_hash: u8, key: &K) -> Option<(&Self, usize)>
    where
        K: Eq,
    {
        let h = self.hashes.into_iter().enumerate().find(|(i, hash)| {
            let bucket_item = &self.get_bucket_value(*i);
            match bucket_item {
                None => false,
                Some(b) => {
                    // we expect the key to be a valid pointer because we own the structure
                    let bucket_key = unsafe { &b.as_ref().expect("Key should not be null here").0 };
                    *hash == highbits_hash && *key == *bucket_key
                }
            }
        });
        match h {
            None => self
                .get_child_bucket() // recursively look in the child bucket
                .and_then(|b| b.find_bucket(highbits_hash, key)),
            // we found the bucket we are looking for
            Some((i, _)) => Some((self, i)),
        }
    }

    // Same as find_bucket but returns a mutable reference to the bucket
    fn find_bucket_mut(&mut self, highbits_hash: u8, key: &K) -> Option<(&mut Self, usize)>
    where
        K: Eq,
    {
        let h = self.hashes.into_iter().enumerate().find(|(i, hash)| {
            let bucket_item = &self.get_bucket_value(*i);
            match bucket_item {
                None => false,
                Some(b) => {
                    // we expect the key to be a valid pointer because we own the structure
                    let bucket_key = unsafe { &b.as_ref().expect("Key should not be null here").0 };
                    *hash == highbits_hash && *key == *bucket_key
                }
            }
        });
        match h {
            None => self
                .get_child_bucket() // recursively look in the child bucket
                .and_then(|b| b.find_bucket_mut(highbits_hash, key)),
            // we found the bucket we are looking for
            Some((i, _)) => Some((self, i)),
        }
    }
    pub fn is_full(&self) -> bool {
        self.meta.trailing_ones() as usize >= BUCKET_SIZE
    }

    /// finds a bucket with the free position and the index of the free
    /// position and returns it.
    ///
    /// Allocates a new bucket if all of the buckets are full. If a new bucket was allocated
    /// the bool is true
    fn find_free_pos(&mut self) -> (&mut Self, usize, bool) {
        // get the trailing ones to find the index first 0
        let first_free = self.meta.trailing_ones() as usize;
        // if the bucket is full, we potentially need to look in the child bucket as well
        if first_free >= BUCKET_SIZE {
            // recursively look in the child bucket

            // here .. we inline the definition of get_child_bucket
            // to get around the borrow checker being mad about
            // early self return
            if self.chained() {
                // position has to be filled if the bucket is chained
                debug_assert!(self.is_pos_filled(BUCKET_SIZE - 1));
                // assert an invariant that when a bucket is chained, the slot is also marked as filled
                unsafe {
                    self.bucket[BUCKET_SIZE - 1]
                        .ptr
                        .as_mut()
                        .expect("Child bucket should not be null when the bucket is chained")
                }
                .find_free_pos()
            } else {
                let last_value = unsafe { self.bucket[BUCKET_SIZE - 1].value };
                let hash = self.hashes[BUCKET_SIZE - 1];
                let mut b = Box::<Self>::default();

                // insert the current bucket's last value
                // into the first position of the newly allocated bucket
                b.meta |= 1;
                b.hashes[0] = hash;
                b.bucket[0] = BucketItem { value: last_value };
                let raw_b = Box::into_raw(b);
                // set the child bucket to the newly allocated bucket
                self.bucket[BUCKET_SIZE - 1] = BucketItem { ptr: raw_b };
                self.flip_chained();
                // SAFETY: we just created the bucket and stored it in the parent bucket .. so we don't need to worry about
                // the pointer being null or invalid here
                let new_b = unsafe { raw_b.as_mut_unchecked() };
                // the first free pos is 1
                (new_b, 1, true)
            }
        } else {
            (self, first_free, false)
        }
    }

    // removes an item from the bucket
    pub fn remove_item(&mut self, index: usize) {
        unimplemented!()
    }
}

impl<K, V> Default for RawBucket<K, V> {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

impl<K, V> Drop for RawBucket<K, V> {
    fn drop(&mut self) {
        let chained = self.chained();
        // TODO: I'm not sure if this works for teh child bucket case.
        for (i, b) in self.bucket.iter_mut().enumerate() {
            // FIXME: We need to also drop the child bucket
            if chained && i == BUCKET_SIZE - 1 {
                unsafe {
                    b.ptr.drop_in_place();
                    Global.deallocate(
                        NonNull::new(b.value).unwrap().cast(),
                        Layout::new::<RawBucket<K, V>>(),
                    );
                }
            } else {
                unsafe {
                    b.value.drop_in_place();
                    Global.deallocate(
                        NonNull::new(b.value).unwrap().cast(),
                        Layout::new::<(K, V)>(),
                    );
                }
            }
        }
    }
}

// a bucketitem could contain a string .. or
// The idea is that a bucket must be 64-bytes to fit in a cache line
#[derive(Clone, Copy)]
union BucketItem<K, V> {
    // A pointer to a pair of key value
    value: *mut (K, V),
    ptr: *mut RawBucket<K, V>,
}

// check at compile time that the size of the bucket is
// indeed 64 bytes
#[allow(unused)]
const fn compile_time_check<K, V>() {
    let size = size_of::<RawBucket<K, V>>();
    if size != 64 {
        panic!("Expect bucket size to be 64");
    }
}
const _: () = compile_time_check::<u64, u64>();

// A bucket that stores hashes and hash values.
// since we are storing strings .. a string is essentially a wrapper to Vec ..
#[cfg(test)]
mod test {
    use crate::kv_store::RawBucket;

    use super::{HashTable, BUCKET_SIZE};

    #[test]
    fn test_meta_funcs() {
        let meta: u8 = 0b10010001;
        let r: RawBucket<(), ()> = RawBucket {
            meta,
            // for now .. we don't care about bucket..
            bucket: unsafe { std::mem::zeroed() },
            hashes: unsafe { std::mem::zeroed() },
        };

        assert!(r.chained());
        assert!(r.is_pos_filled(4));
        assert!(r.is_pos_filled(0));
        assert!(!r.is_pos_filled(3));
        assert!(!r.is_bucket_full());

        // Interesting invariant here w/ this representation..
        // if the bucket is chained .. then the 7th bit must be 0
        let meta: u8 = 0b10111111;
        let r: RawBucket<(), ()> = RawBucket {
            meta,
            // for now .. we don't care about bucket..
            bucket: unsafe { std::mem::zeroed() },
            hashes: unsafe { std::mem::zeroed() },
        };
        assert!(r.is_bucket_full());

        // the behavior of the 7th bit doesn't matter
        // when the bucket is chained.
        let meta: u8 = 0b11111111;
        let r: RawBucket<(), ()> = RawBucket {
            meta,
            // for now .. we don't care about bucket..
            bucket: unsafe { std::mem::zeroed() },
            hashes: unsafe { std::mem::zeroed() },
        };
        assert!(r.is_bucket_full());

        let meta: u8 = 0b01111111;
        let r: RawBucket<(), ()> = RawBucket {
            meta,
            // for now .. we don't care about bucket..
            bucket: unsafe { std::mem::zeroed() },
            hashes: unsafe { std::mem::zeroed() },
        };
        assert!(r.is_bucket_full());
    }

    #[test]
    fn test_raw_bucket_simple() {
        let mut raw_bucket: RawBucket<u64, u64> = Default::default();
        // this should fill the bucket
        for i in 0..BUCKET_SIZE {
            raw_bucket.insert_item(i as u64, i as u64 + 5, i as u64);
        }
        assert!(raw_bucket.is_full(), "bucket should be full");
        for i in 0..BUCKET_SIZE {
            let u = i as u64;
            let v = raw_bucket.get(u, &u);
            // we should be able to get the value back
            assert!(v.is_some());
            assert_eq!(*v.unwrap(), u + 5);
        }

        // insert more items with different hashes
        for i in BUCKET_SIZE..BUCKET_SIZE * 2 - 1 {
            raw_bucket.insert_item(i as u64, i as u64 + 5, i as u64);
        }

        // // this is really testing an implementation detail .. but yea
        // assert_eq!(
        //     raw_bucket.bucket_levels(),
        //     2,
        //     "bucket should have two levels"
        // );
        //
        // for i in BUCKET_SIZE..BUCKET_SIZE * 2 - 1 {
        //     let u = i as u64;
        //     let v = raw_bucket.get(u, &u);
        //     // we should be able to get the value back
        //     assert!(v.is_some());
        //     assert_eq!(*v.unwrap(), u + 5);
        //
        //     // we can also check that these values are stored in the second level
        //     let child = raw_bucket.get_child_bucket().unwrap();
        //     let v = child.get(u, &u);
        //     assert!(v.is_some(), "v is : {v:?}, u is: {u}");
        //     assert_eq!(*v.unwrap(), u + 5);
        // }

        // This shouldn't leak..?
    }
}
