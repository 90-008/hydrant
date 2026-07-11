use std::collections::BTreeMap;
use std::io::Cursor;

use bytes::Bytes;
use cid::Cid as IpldCid;
use miette::{IntoDiagnostic, Result};

#[cfg_attr(not(feature = "indexer"), allow(dead_code))]
pub(crate) fn parse_car_blocks(data: Bytes) -> Result<BTreeMap<IpldCid, Bytes>> {
    let mut offset = 0;
    let Some(header_len) = read_uvarint(&data, &mut offset)? else {
        return Err(miette::miette!("empty CAR file"));
    };
    let header_end = offset
        .checked_add(header_len)
        .ok_or_else(|| miette::miette!("CAR header length overflow"))?;
    if header_end > data.len() {
        return Err(miette::miette!("truncated CAR header"));
    }
    offset = header_end;

    let mut blocks = BTreeMap::new();
    while let Some(section_len) = read_uvarint(&data, &mut offset)? {
        let section_end = offset
            .checked_add(section_len)
            .ok_or_else(|| miette::miette!("CAR block length overflow"))?;
        if section_end > data.len() {
            return Err(miette::miette!("truncated CAR block"));
        }

        let section = data.slice(offset..section_end);
        offset = section_end;

        let mut cursor = Cursor::new(section.as_ref());
        let cid = IpldCid::read_bytes(&mut cursor).into_diagnostic()?;
        let block_start = cursor.position() as usize;
        if block_start >= section.len() {
            return Err(miette::miette!("CAR block has no payload for {cid}"));
        }
        blocks.insert(cid, section.slice(block_start..));
    }

    Ok(blocks)
}

/// validates that each block's payload hashes to its claimed CID (sha2-256, dag-cbor).
pub(crate) fn validate_block_cids<'a>(
    blocks: impl IntoIterator<Item = (&'a IpldCid, &'a Bytes)>,
) -> Result<()> {
    for (claimed_cid, bytes) in blocks {
        let computed_cid =
            jacquard_repo::mst::util::compute_cid(bytes.as_ref()).into_diagnostic()?;
        if computed_cid != *claimed_cid {
            return Err(miette::miette!(
                "CAR block CID mismatch: claimed {claimed_cid}, computed {computed_cid}"
            ));
        }
    }
    Ok(())
}

#[cfg_attr(not(feature = "indexer"), allow(dead_code))]
fn read_uvarint(data: &[u8], offset: &mut usize) -> Result<Option<usize>> {
    if *offset == data.len() {
        return Ok(None);
    }

    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        if *offset >= data.len() {
            return Err(miette::miette!("truncated uvarint in CAR file"));
        }

        let byte = data[*offset];
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            let len = usize::try_from(value).into_diagnostic()?;
            return Ok(Some(len));
        }
    }

    Err(miette::miette!("uvarint overflow in CAR file"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::multihash::Multihash;
    use jacquard_common::types::crypto::{DAG_CBOR, SHA2_256};

    fn cid(byte: u8) -> IpldCid {
        let hash = [byte; 32];
        let mh = Multihash::<64>::wrap(SHA2_256, &hash).unwrap();
        IpldCid::new_v1(DAG_CBOR, mh)
    }

    #[tokio::test]
    async fn parses_rootless_block_car() {
        let cid = cid(1);
        let mut buf = Vec::new();
        let header = iroh_car::CarHeader::new_v1(Vec::new());
        let mut writer = iroh_car::CarWriter::new(header, &mut buf);
        writer.write(cid, b"block".to_vec()).await.unwrap();
        writer.finish().await.unwrap();

        let blocks = parse_car_blocks(buf.into()).unwrap();
        assert_eq!(blocks.get(&cid).unwrap().as_ref(), b"block");
    }

    #[test]
    fn validate_block_cids_rejects_mismatched_cid() {
        let blocks = BTreeMap::from([(cid(1), Bytes::from_static(b"forged"))]);
        let err = validate_block_cids(&blocks).unwrap_err();
        assert!(err.to_string().contains("CAR block CID mismatch"));
    }

    #[test]
    fn validate_block_cids_accepts_matching_cid() {
        let bytes = Bytes::from_static(b"trusted");
        let claimed = jacquard_repo::mst::util::compute_cid(bytes.as_ref()).unwrap();
        let blocks = BTreeMap::from([(claimed, bytes)]);
        validate_block_cids(&blocks).unwrap();
    }
}
