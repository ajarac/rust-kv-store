#[derive(Clone)]
pub struct Versioned {
    pub ver: u64,
    pub tomb: bool,
    pub val: Vec<u8>,
}

impl Versioned {
    pub fn from_value(version: u64, value: &[u8]) -> Self {
        Versioned {
            ver: version,
            tomb: false,
            val: value.to_vec(),
        }
    }
    pub fn tomb(version: u64) -> Self {
        Versioned {
            ver: version,
            tomb: true,
            val: Vec::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Versioned;

    #[test]
    fn from_value_should_create_not_tomb_value() {
        let versioned = Versioned::from_value(0, b"foo");
        assert_eq!(versioned.tomb, false);
        assert_eq!(versioned.ver, 0);
        assert_eq!(versioned.val, b"foo".to_vec());
    }

    #[test]
    fn from_value_should_create_tomb_value() {
        let versioned = Versioned::tomb(0);
        assert_eq!(versioned.tomb, true);
        assert_eq!(versioned.ver, 0);
        assert_eq!(versioned.val, b"".to_vec());
    }
}
