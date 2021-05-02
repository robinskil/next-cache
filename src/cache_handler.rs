use std::{sync::Arc, u8};

use dashmap::DashMap;

pub fn insert_cache_line(
    cache: Arc<DashMap<String, Vec<u8>>>,
    key: String,
    value: Vec<u8>,
) -> CacheInteractionResult {
    if cache.contains_key(&key) {
        CacheInteractionResult::AnotherCacheEntryExists
    } else {
        cache.insert(key, value);
        CacheInteractionResult::InsertSucces
    }
}

pub fn get_cache_line(cache: Arc<DashMap<String, Vec<u8>>>, key: String) -> CacheInteractionResult {
    let cache_line = cache.get(key.as_str());
    match cache_line {
        Some(value) => {
            let val = value.value();
            CacheInteractionResult::Found(val.clone())
        }
        None => CacheInteractionResult::NoEntryFound,
    }
}

#[derive(PartialEq, Debug)]
pub enum CacheInteractionResult {
    Found(Vec<u8>),
    NoEntryFound,
    InsertSucces,
    AnotherCacheEntryExists,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache_handler::{self, CacheInteractionResult};

    #[test]
    fn insert_ok() {
        let mut bytes = vec![1u8];
        let cache = Arc::new(dashmap::DashMap::new());
        let entry_res = cache_handler::insert_cache_line(
            cache.clone(),
            String::from("f227cc6023c34cf0ad7b27adfd2068b2"),
            bytes,
        );
        assert_eq!(entry_res, CacheInteractionResult::InsertSucces);
    }

    #[test]
    fn insert_entry_already_exists() {
        let mut bytes = vec![1u8];
        let cache = Arc::new(dashmap::DashMap::new());
        let first_entry = cache_handler::insert_cache_line(
            cache.clone(),
            String::from("f227cc6023c34cf0ad7b27adfd2068b2"),
            bytes.clone(),
        );
        let second_entry = cache_handler::insert_cache_line(
            cache.clone(),
            String::from("f227cc6023c34cf0ad7b27adfd2068b2"),
            bytes.clone(),
        );
        assert_eq!(first_entry, CacheInteractionResult::InsertSucces);
        assert_eq!(
            second_entry,
            CacheInteractionResult::AnotherCacheEntryExists
        );
    }

    #[test]
    fn insert_and_retrieve_entry() {
        let mut bytes = vec![1u8];
        let cache = Arc::new(dashmap::DashMap::new());
        let entry = cache_handler::insert_cache_line(
            cache.clone(),
            String::from("f227cc6023c34cf0ad7b27adfd2068b2"),
            bytes.clone(),
        );
        let found_entry = cache_handler::get_cache_line(
            cache.clone(),
            String::from("f227cc6023c34cf0ad7b27adfd2068b2"),
        );
        assert_eq!(entry, CacheInteractionResult::InsertSucces);
        match found_entry {
            CacheInteractionResult::Found(stream) => {
                assert_eq!(stream, bytes);
            }
            _ => {
                panic!("Expected a found entry")
            }
        }
    }
}
