use fjall::UserKey;
use jacquard::{CowStr, IntoStatic};
use jacquard_common::types::string::Did;
use miette::{Context, IntoDiagnostic, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smol_str::format_smolstr;

#[derive(Clone, Debug)]
pub struct TrimmedDid<'s>(CowStr<'s>);

impl<'s> TrimmedDid<'s> {
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn into_static(self) -> TrimmedDid<'static> {
        TrimmedDid(self.0.into_static())
    }

    pub fn to_did(&self) -> Did<'static> {
        Did::new_owned(format_smolstr!("did:{}", self.0)).expect("expected valid trimmed did")
    }
}

impl<'a> TryFrom<&'a [u8]> for TrimmedDid<'a> {
    type Error = miette::Report;

    fn try_from(value: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(value)
            .into_diagnostic()
            .wrap_err("expected trimmed did to be valid utf-8")?;

        // validate using Did::new with stack-allocated buffer
        const PREFIX: &[u8] = b"did:";
        const MAX_DID_LEN: usize = 2048;
        let full_len = PREFIX.len() + value.len();
        if full_len > MAX_DID_LEN {
            miette::bail!("trimmed did too long");
        }
        let mut buf = [0u8; MAX_DID_LEN];
        buf[..PREFIX.len()].copy_from_slice(PREFIX);
        buf[PREFIX.len()..full_len].copy_from_slice(value);
        let full_did = std::str::from_utf8(&buf[..full_len]).expect("already validated utf-8");
        Did::new(full_did)
            .into_diagnostic()
            .wrap_err("expected trimmed did to be valid did")?;

        Ok(TrimmedDid(CowStr::Borrowed(s)))
    }
}

impl<'a> AsRef<[u8]> for TrimmedDid<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> Into<UserKey> for TrimmedDid<'a> {
    fn into(self) -> UserKey {
        UserKey::new(self.as_bytes())
    }
}

impl<'a> Into<UserKey> for &TrimmedDid<'a> {
    fn into(self) -> UserKey {
        UserKey::new(self.as_bytes())
    }
}

impl<'a> From<&'a Did<'a>> for TrimmedDid<'a> {
    fn from(did: &'a Did<'a>) -> Self {
        TrimmedDid(CowStr::Borrowed(did.as_str().trim_start_matches("did:")))
    }
}

impl Serialize for TrimmedDid<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TrimmedDid<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <&'de str>::deserialize(deserializer)?;
        Ok(TrimmedDid(CowStr::Borrowed(s)))
    }
}
