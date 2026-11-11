# SPDX-License-Identifier: Apache-2.0

"""age (v1) sealing here, for Secret.encrypted: the value never leaves this
process, only its ciphertext does.

X25519 recipients (age1...) and the ASCII armor zctl writes (the Go
agepayload package), which the agent opens with filippo.io/age.
Only seals: opening is the agent's. Needs cryptography
(pip install 'zenthros[seal]').

The format (age-encryption.org/v1): a 16-byte file key, wrapped for each
recipient with ChaCha20-Poly1305 under HKDF-SHA256 of an X25519 exchange
with a fresh ephemeral key; a header HMAC under a key derived from the file
key; the payload in STREAM chunks of 64 KiB, ChaCha20-Poly1305 under a key
derived from the file key and a 16-byte nonce.
"""

import base64
import hashlib
import hmac
import os

from zenthros import errors

MAX_PLAINTEXT = 32 * 1024
MAX_CIPHERTEXT = 64 * 1024
MAX_RECIPIENTS = 8
ARMOR_BEGIN = "-----BEGIN AGE ENCRYPTED FILE-----"
ARMOR_END = "-----END AGE ENCRYPTED FILE-----"
_CHUNK = 64 * 1024
_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def available():
    """Whether this Python can seal: cryptography is installed."""
    try:
        import cryptography.hazmat.primitives.asymmetric.x25519  # noqa: F401
    except ImportError:
        return False
    return True


def _polymod(values):
    generator = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    check = 1
    for value in values:
        top = check >> 25
        check = (check & 0x1FFFFFF) << 5 ^ value
        for i in range(5):
            check ^= generator[i] if (top >> i) & 1 else 0
    return check


def _hrp_expand(hrp):
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]


def _convert(data, source, target, pad):
    acc, bits, out, top = 0, 0, [], (1 << target) - 1
    for value in data:
        acc = (acc << source) | value
        bits += source
        while bits >= target:
            bits -= target
            out.append((acc >> bits) & top)
    if pad and bits:
        out.append((acc << (target - bits)) & top)
    elif not pad and (bits >= source or (acc << (target - bits)) & top):
        raise ValueError("bech32: non-zero padding")
    return out


def bech32_encode(hrp, data):
    """data (bytes) under hrp, as age writes its keys (lower case)."""
    words = _convert(data, 8, 5, True)
    check = _polymod(_hrp_expand(hrp) + words + [0] * 6) ^ 1
    return hrp + "1" + "".join(_CHARSET[w] for w in words + [(check >> 5 * (5 - i)) & 31 for i in range(6)])


def bech32_decode(text):
    """(hrp, data bytes) of a bech32 string, checksum checked; all lower or
    all upper case, as age takes it."""
    if text.lower() != text and text.upper() != text:
        raise ValueError("bech32: mixed case")
    text = text.lower()
    split = text.rfind("1")
    if split < 1 or len(text) - split < 7:
        raise ValueError("bech32: no separator or checksum")
    hrp, words = text[:split], []
    for c in text[split + 1:]:
        if c not in _CHARSET:
            raise ValueError(f"bech32: {c!r} is not in the alphabet")
        words.append(_CHARSET.index(c))
    if _polymod(_hrp_expand(hrp) + words) != 1:
        raise ValueError("bech32: bad checksum")
    return hrp, bytes(_convert(words[:-6], 5, 8, False))


def recipient_key(recipient):
    """The X25519 public key of an age1 recipient."""
    try:
        hrp, key = bech32_decode(recipient)
    except ValueError as exc:
        raise errors.ValidationError(f"recipient {recipient!r} is not an age1 X25519 key: {exc}") from None
    if hrp != "age" or len(key) != 32:
        raise errors.ValidationError(f"recipient {recipient!r} is not an age1 X25519 key")
    return key


def key_id(recipient):
    """A short public fingerprint of a recipient, as zctl writes keyId."""
    return recipient[len("age1"):][:12]


def _b64(data):
    return base64.b64encode(data).decode().rstrip("=")


def _hkdf(key, salt, info):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    return HKDF(algorithm=hashes.SHA256(), length=32, salt=salt or None, info=info).derive(key)


def _seal_chunk(key, nonce, data):
    from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

    return ChaCha20Poly1305(key).encrypt(nonce, data, None)


def _stanza(file_key, recipient, random):
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey

    theirs = recipient_key(recipient)
    ephemeral = X25519PrivateKey.from_private_bytes(random(32))
    share = ephemeral.public_key().public_bytes_raw()
    try:
        shared = ephemeral.exchange(X25519PublicKey.from_public_bytes(theirs))
    except ValueError:
        # An all-zero shared secret: a low-order point, no key at all.
        raise errors.ValidationError(f"recipient {recipient!r} is a low-order point, not a key") from None
    wrap = _hkdf(shared, share + theirs, b"age-encryption.org/v1/X25519")
    body = _seal_chunk(wrap, bytes(12), file_key)
    return f"-> X25519 {_b64(share)}\n{_b64(body)}\n"


def seal(plaintext, recipients, random=os.urandom):
    """plaintext (bytes or str) sealed to the age1 recipients, ASCII armored.
    random is the randomness (a test makes it fixed)."""
    if isinstance(plaintext, str):
        plaintext = plaintext.encode()
    plaintext = bytes(plaintext)
    if not plaintext:
        raise errors.ValidationError("an empty secret has nothing to seal")
    if len(plaintext) > MAX_PLAINTEXT:
        raise errors.ValidationError(f"a sealed secret is at most {MAX_PLAINTEXT >> 10} KiB")
    unique = list(dict.fromkeys(recipients))
    if not unique or len(unique) > MAX_RECIPIENTS:
        raise errors.ValidationError(f"seal to 1 to {MAX_RECIPIENTS} recipients, not {len(unique)}")
    for recipient in unique:
        recipient_key(recipient)
    file_key = random(16)
    header = "age-encryption.org/v1\n" + "".join(_stanza(file_key, r, random) for r in unique) + "---"
    mac = hmac.new(_hkdf(file_key, b"", b"header"), header.encode(), hashlib.sha256).digest()
    nonce = random(16)
    key = _hkdf(file_key, nonce, b"payload")
    chunks = [plaintext[i:i + _CHUNK] for i in range(0, len(plaintext), _CHUNK)]
    payload = b"".join(_seal_chunk(key, i.to_bytes(11, "big") + (b"\x01" if i == len(chunks) - 1 else b"\x00"), chunk)
                       for i, chunk in enumerate(chunks))
    raw = f"{header} {_b64(mac)}\n".encode() + nonce + payload
    encoded = base64.b64encode(raw).decode()
    armored = "\n".join([ARMOR_BEGIN] + [encoded[i:i + 64] for i in range(0, len(encoded), 64)] + [ARMOR_END]) + "\n"
    if len(armored) > MAX_CIPHERTEXT:
        raise errors.ValidationError(f"the sealed secret is over {MAX_CIPHERTEXT >> 10} KiB")
    return armored
