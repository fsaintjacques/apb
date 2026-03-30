//! Low-level protobuf wire format encoding helpers.
//!
//! We write raw bytes directly instead of depending on prost's encoding,
//! so the transcoder controls the exact output format.

/// Protobuf wire types.
pub const WIRE_VARINT: u8 = 0;
pub const WIRE_FIXED64: u8 = 1;
pub const WIRE_LENGTH_DELIMITED: u8 = 2;
pub const WIRE_FIXED32: u8 = 5;

/// Encode a varint (unsigned LEB128) into the buffer.
#[inline]
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Encode a signed 32-bit value as a zigzag-encoded varint.
#[inline]
pub fn encode_zigzag32(value: i32, buf: &mut Vec<u8>) {
    let encoded = ((value << 1) ^ (value >> 31)) as u32;
    encode_varint(encoded as u64, buf);
}

/// Encode a signed 64-bit value as a zigzag-encoded varint.
#[inline]
pub fn encode_zigzag64(value: i64, buf: &mut Vec<u8>) {
    let encoded = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(encoded, buf);
}

/// Encode a fixed 32-bit value (little-endian).
#[inline]
pub fn encode_fixed32(value: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a fixed 64-bit value (little-endian).
#[inline]
pub fn encode_fixed64(value: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a length-delimited value (varint length prefix + data).
#[inline]
pub fn encode_length_delimited(data: &[u8], buf: &mut Vec<u8>) {
    encode_varint(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

/// Encode a proto field tag (field_number << 3 | wire_type).
#[inline]
pub fn encode_tag_to(field_number: u32, wire_type: u8, buf: &mut Vec<u8>) {
    let tag = ((field_number as u64) << 3) | (wire_type as u64);
    encode_varint(tag, buf);
}

/// Pre-encode a proto field tag as bytes.
pub fn encode_tag(field_number: u32, wire_type: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2);
    encode_tag_to(field_number, wire_type, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_single_byte() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf, [0x00]);

        buf.clear();
        encode_varint(1, &mut buf);
        assert_eq!(buf, [0x01]);

        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf, [0x7F]);
    }

    #[test]
    fn varint_multi_byte() {
        let mut buf = Vec::new();
        encode_varint(128, &mut buf);
        assert_eq!(buf, [0x80, 0x01]);

        buf.clear();
        encode_varint(300, &mut buf);
        assert_eq!(buf, [0xAC, 0x02]);

        buf.clear();
        encode_varint(150, &mut buf);
        // 150 = 0b10010110 → 0x96 0x01
        assert_eq!(buf, [0x96, 0x01]);
    }

    #[test]
    fn zigzag32() {
        let mut buf = Vec::new();
        encode_zigzag32(0, &mut buf);
        assert_eq!(buf, [0x00]);

        buf.clear();
        encode_zigzag32(-1, &mut buf);
        assert_eq!(buf, [0x01]);

        buf.clear();
        encode_zigzag32(1, &mut buf);
        assert_eq!(buf, [0x02]);

        buf.clear();
        encode_zigzag32(-2, &mut buf);
        assert_eq!(buf, [0x03]);
    }

    #[test]
    fn fixed32_le() {
        let mut buf = Vec::new();
        encode_fixed32(1, &mut buf);
        assert_eq!(buf, [0x01, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn fixed64_le() {
        let mut buf = Vec::new();
        encode_fixed64(1, &mut buf);
        assert_eq!(buf, [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn length_delimited() {
        let mut buf = Vec::new();
        encode_length_delimited(b"hello", &mut buf);
        assert_eq!(buf, [5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn tag_encoding() {
        // field 1, varint → (1 << 3) | 0 = 8
        assert_eq!(encode_tag(1, WIRE_VARINT), [0x08]);
        // field 2, length-delimited → (2 << 3) | 2 = 18
        assert_eq!(encode_tag(2, WIRE_LENGTH_DELIMITED), [0x12]);
        // field 1, fixed32 → (1 << 3) | 5 = 13
        assert_eq!(encode_tag(1, WIRE_FIXED32), [0x0D]);
    }
}
