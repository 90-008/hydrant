use data_encoding::BASE32_NOPAD;
use fjall::UserKey;
use jacquard::{CowStr, IntoStatic};
use jacquard_common::types::string::Did;
use jacquard_common::types::tid::Tid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smol_str::{SmolStr, SmolStrBuilder, format_smolstr};
use std::fmt::Display;

const S32_CHAR: &str = "234567abcdefghijklmnopqrstuvwxyz";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrimmedDid<'s> {
    Plc([u8; 15]),
    Web(CowStr<'s>),
    Other(CowStr<'s>),
}

const TAG_PLC: u8 = 1;
const TAG_WEB: u8 = 2;

impl<'s> TrimmedDid<'s> {
    pub fn len(&self) -> usize {
        match self {
            TrimmedDid::Plc(_) => 16,
            TrimmedDid::Web(s) => 1 + s.len(),
            TrimmedDid::Other(s) => s.len(),
        }
    }

    pub fn into_static(self) -> TrimmedDid<'static> {
        match self {
            TrimmedDid::Plc(bytes) => TrimmedDid::Plc(bytes),
            TrimmedDid::Web(s) => TrimmedDid::Web(s.into_static()),
            TrimmedDid::Other(s) => TrimmedDid::Other(s.into_static()),
        }
    }

    pub fn to_did(&self) -> Did<'static> {
        match self {
            TrimmedDid::Plc(_) => {
                let s = self.to_string();
                Did::new_owned(format_smolstr!("did:{}", s)).expect("valid did from plc")
            }
            TrimmedDid::Web(s) => {
                Did::new_owned(format_smolstr!("did:web:{}", s)).expect("valid did from web")
            }
            TrimmedDid::Other(s) => {
                Did::new_owned(format_smolstr!("did:{}", s)).expect("valid did from other")
            }
        }
    }

    pub fn write_to_vec(&self, buf: &mut Vec<u8>) {
        match self {
            TrimmedDid::Plc(bytes) => {
                buf.push(TAG_PLC);
                buf.extend_from_slice(bytes);
            }
            TrimmedDid::Web(s) => {
                buf.push(TAG_WEB);
                buf.extend_from_slice(s.as_bytes());
            }
            TrimmedDid::Other(s) => buf.extend_from_slice(s.as_bytes()),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            TrimmedDid::Plc(bytes) => {
                let mut s = String::with_capacity(28);
                s.push_str("plc:");
                s.push_str(&BASE32_NOPAD.encode(bytes).to_ascii_lowercase());
                s
            }
            TrimmedDid::Web(s) => {
                format!("web:{}", s)
            }
            TrimmedDid::Other(s) => s.to_string(),
        }
    }
}

impl Display for TrimmedDid<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimmedDid::Plc(bytes) => {
                f.write_str("plc:")?;
                f.write_str(&BASE32_NOPAD.encode(bytes).to_ascii_lowercase())
            }
            TrimmedDid::Web(s) => {
                f.write_str("web:")?;
                f.write_str(s)
            }
            TrimmedDid::Other(s) => f.write_str(s),
        }
    }
}

impl<'a> From<&'a Did<'a>> for TrimmedDid<'a> {
    fn from(did: &'a Did<'a>) -> Self {
        let s = did.as_str();
        if let Some(rest) = s.strip_prefix("did:plc:") {
            if rest.len() == 24 {
                // decode
                if let Ok(bytes) = BASE32_NOPAD.decode(rest.to_ascii_uppercase().as_bytes()) {
                    if bytes.len() == 15 {
                        return TrimmedDid::Plc(bytes.try_into().unwrap());
                    }
                }
            }
        } else if let Some(rest) = s.strip_prefix("did:web:") {
            return TrimmedDid::Web(CowStr::Borrowed(rest));
        }
        TrimmedDid::Other(CowStr::Borrowed(s.trim_start_matches("did:")))
    }
}

impl<'a> TryFrom<&'a [u8]> for TrimmedDid<'a> {
    type Error = miette::Report;

    fn try_from(value: &'a [u8]) -> miette::Result<Self> {
        if value.is_empty() {
            miette::bail!("empty did key");
        }
        match value[0] {
            TAG_PLC => {
                if value.len() == 16 {
                    let mut arr = [0u8; 15];
                    arr.copy_from_slice(&value[1..]);
                    return Ok(TrimmedDid::Plc(arr));
                }
                miette::bail!("invalid length for tagged plc did");
            }
            TAG_WEB => {
                if let Ok(s) = std::str::from_utf8(&value[1..]) {
                    return Ok(TrimmedDid::Web(CowStr::Borrowed(s)));
                }
                miette::bail!("invalid utf8 for tagged web did");
            }
            _ => {
                if let Ok(s) = std::str::from_utf8(value) {
                    return Ok(TrimmedDid::Other(CowStr::Borrowed(s)));
                }
                miette::bail!("invalid utf8 for other did");
            }
        }
    }
}

impl<'a> Into<UserKey> for TrimmedDid<'a> {
    fn into(self) -> UserKey {
        let mut vec = Vec::with_capacity(32);
        self.write_to_vec(&mut vec);
        UserKey::new(&vec)
    }
}

impl<'a> Into<UserKey> for &TrimmedDid<'a> {
    fn into(self) -> UserKey {
        let mut vec = Vec::with_capacity(32);
        self.write_to_vec(&mut vec);
        UserKey::new(&vec)
    }
}

impl Serialize for TrimmedDid<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut vec = Vec::with_capacity(32);
        self.write_to_vec(&mut vec);
        serializer.serialize_bytes(&vec)
    }
}

impl<'de> Deserialize<'de> for TrimmedDid<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DidVisitor;
        impl<'de> serde::de::Visitor<'de> for DidVisitor {
            type Value = TrimmedDid<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bytes (tagged) or string (legacy)")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TrimmedDid::try_from(v)
                    .map(|td| td.into_static())
                    .map_err(E::custom)
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TrimmedDid::try_from(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(DidVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct DbTid(u64);

impl DbTid {
    pub fn new(tid: u64) -> Self {
        Self(tid)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_tid(&self) -> Tid {
        Tid::raw(self.to_smolstr())
    }

    fn to_smolstr(&self) -> SmolStr {
        let mut i = self.0;
        let mut s = SmolStrBuilder::new();
        for _ in 0..13 {
            let c = i & 0x1F;
            s.push(S32_CHAR.chars().nth(c as usize).unwrap());
            i >>= 5;
        }

        let mut builder = SmolStrBuilder::new();
        for c in s.finish().chars().rev() {
            builder.push(c);
        }
        builder.finish()
    }
}

impl From<&Tid> for DbTid {
    fn from(tid: &Tid) -> Self {
        DbTid(jacquard_common::types::tid::s32decode(tid.to_string()))
    }
}

impl From<Tid> for DbTid {
    fn from(tid: Tid) -> Self {
        DbTid::from(&tid)
    }
}

impl From<DbTid> for Tid {
    fn from(val: DbTid) -> Self {
        val.to_tid()
    }
}

impl Display for DbTid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_smolstr())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DbAction {
    Create = 0,
    Update = 1,
    Delete = 2,
}

impl DbAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            DbAction::Create => "create",
            DbAction::Update => "update",
            DbAction::Delete => "delete",
        }
    }
}

impl<'a> From<&'a str> for DbAction {
    fn from(s: &'a str) -> Self {
        match s {
            "create" => DbAction::Create,
            "update" => DbAction::Update,
            "delete" => DbAction::Delete,
            _ => panic!("invalid action: {}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DbRkey {
    Tid(DbTid),
    Str(SmolStr),
}

impl DbRkey {
    pub fn new(s: &str) -> Self {
        if let Ok(tid) = Tid::new(s) {
            DbRkey::Tid(DbTid::from(tid))
        } else {
            DbRkey::Str(SmolStr::from(s))
        }
    }

    pub fn to_smolstr(&self) -> SmolStr {
        match self {
            DbRkey::Tid(tid) => tid.to_smolstr(),
            DbRkey::Str(s) => s.clone(),
        }
    }
}

impl From<&str> for DbRkey {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for DbRkey {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl From<SmolStr> for DbRkey {
    fn from(s: SmolStr) -> Self {
        Self::new(s.as_str())
    }
}

impl Display for DbRkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_smolstr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jacquard_common::types::tid::Tid;

    #[test]
    fn test_dbtid_roundtrip() {
        let tid_str = "3jzfcijpj2z2a";
        let tid = Tid::new(tid_str).unwrap();
        let db_tid = DbTid::from(&tid);
        assert_eq!(db_tid.to_tid().as_str(), tid_str);

        let tid_str_2 = "2222222222222";
        let tid = Tid::new(tid_str_2).unwrap();
        let db_tid = DbTid::from(&tid);
        assert_eq!(db_tid.to_tid().as_str(), tid_str_2);
    }

    #[test]
    fn test_dbrkey() {
        let tid_str = "3jzfcijpj2z2a";
        let rkey = DbRkey::new(tid_str);
        if let DbRkey::Tid(t) = rkey {
            assert_eq!(t.to_tid().as_str(), tid_str);
        } else {
            panic!("expected tid");
        }

        let str_val = "self";
        let rkey = DbRkey::new(str_val);
        if let DbRkey::Str(s) = rkey {
            assert_eq!(s, str_val);
        } else {
            panic!("expected str");
        }
    }
}
