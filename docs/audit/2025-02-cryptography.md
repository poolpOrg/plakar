---
title: "Audit of Plakar cryptography"
date: 2025-02-28 21:00:00 +0100
authors:
 - "gilles"
---

Before releasing a usable version,
we wanted an expert to examine our cryptographic design and confirm we hadn’t made any regrettable choices.
We were delighted to have [Jean-Philippe Aumasson](https://www.aumasson.jp/) take care of the review—a true privilege given the high level of confidence we have in his skills.

_Below is the unedited review after of our original submission,_
_followed by the unedited remediation review after our corrective steps._
_Comments are inlined in to provide clarifications where needed._

---


## Initial review

### Summary

[Plakar](https://github.com/PlakarKorp/plakar/tree/main) is a data backup solution featuring client-side encryption,
and a server-side deduplication mechanism.

The goal of this audit is to review:

- the soundness of the cryptography architecture
- the reliability of the algorithms and protocols chosen
- the security of the implementation
- the correctness of the documentation

Resources provided by Plakar include:

- the code in [encryption/symmetric.go](https://github.com/PlakarKorp/plakar/blob/main/encryption/symmetric.go)
- documentation in [CRYPTOGRAPHY.md](https://github.com/PlakarKorp/plakar/blob/204f17deff8aeb297c4d23e481bfe9fa36ac363e/CRYPTOGRAPHY.md) and [README.md](https://github.com/PlakarKorp/plakar/blob/204f17deff8aeb297c4d23e481bfe9fa36ac363e//encryption/README.md)

Our general assessment is that the current design is cryptographically sound in terms of components choice and parameters.
However, we propose a number of improvements to reduce security risks, improve performance,
and rely on more state-of-the-art components.

The 3 sections below describes 
1. our observations on the design 
2. our observations on the and code
3. our review of the changes after sharing 1. and 2.

Our observations don't include any major security issue, but instead
recommendations in terms of robustness and performance.
The review of the changes validated the approach chosen, the choice of
algorithms, and their parameters.


---
### Design

#### Password hashing: Scrypt vs. Argon2id

**We recommend switching to Argon2id for password hashing.**

Currently password hashing is done with scrypt, with the [following
parameters](https://github.com/PlakarKorp/plakar/blob/506a90863ce5ef1c70a8e42cb52a8b791eec3c65/encryption/symmetric.go#L36-L41):

```go
    KDFParams: KDFParams{
            N:      1 << 15,
            R:      8,
            P:      1,
            KeyLen: 32,
```

scrypt was developed in 2009 as one of the first memory-hard
password-based hashing schemes with tunable memory. However, Argon2id
was developed through the Password Hashing Competition to address some
of its shortcomings, and is now recommended by modern security
guidelines (such as OWASP and NIST).

Argon2id is defined in [RFC
9106](https://datatracker.ietf.org/doc/rfc9106/). Compared to scrypt, it
has

- Better resistance to side-channel attacks
- More intuitive parameterization
- A simpler internal logic (instead of scrypt's requirements for PBKDF2,
  SHA-2, ChaCha, etc.)

A Go implementation of Argon2id is available in the
[x/crypto](https://pkg.go.dev/golang.org/x/crypto/argon2) package.

We recommend parameters `t = 4` and `m = 256MB`, for a 256 megabyte
usage. If `t = 4` makes hashing too slow, then use `t = 3`.


> note from the developers:\
> The KDF API was refactored so that it can use Argon2Id by default and alter its parameters or switch to a different KDF should it be required.

---
#### Chunk encryption: AES-GCM vs. AES-GCM-SIV

**We recommend switching to AES-GCM-SIV for chunk encryption.**

AES-GCM-SIV is a mode defined in [RFC
8452](https://www.rfc-editor.org/rfc/rfc8452.html) that does not rely on
randomness. It produces the nonce by computing a PRF over the message to
encrypt. It implies that encrypting the same message twice will produce
the same ciphertext. However, if each subkey encrypts a single chunk,
this is not an issue.

AES-GCM-SIV also prevents streaming of the data hashed, therefore the
whole chunk has to be stored in memory. Since data is already chunked to
be streamed, and chunks are of fixed, small size (64 KB), this is not an
issue.

That said, AES-GCM-SIV has less adoption than AES-GCM, and is not as
standardized as AES-GCM. AES-GCM is fine security-wise, switching to the
SIV mode would just eliminate one risk related to randomness. Depending
on the business requirements and client needs, AES-GCM may be preferable
(for example, if a FIPS standard is needed).

> note from the developers:\
> The encryption API was refactored so that it can use AES-GCM-SIV by default and switch to AES-GCM should it be required.

The most reliable implementation of AES-GCM-SIV is in [Google's Tink
package](https://pkg.go.dev/github.com/google/tink/go@v1.7.0/aead/subtle#AESGCMSIV).

Note that the Go language maintainers are planning to [add AES-GCM-SIV
to
x/crypto](https://github.com/golang/go/issues/54364#issuecomment-2460514455).

---
#### Subkey encryption: AES-GCM vs. AES-KW

**We recommend switching to AES-KW for subkey encryption.**

Currently subkeys are encrypted with AES-GCM. However, there is a
dedicated construction for the specific problem of encrypting symmetric
keys (as short, fixed size, high entropy values), namely *key wrapping*. 

Switching to AES-KW would eliminate the need of repeated nonces when
encrypting a large number of subkeys with the same key. Nonce being
12-byte, or 96-bit, a collision of nonces is expected after
approximately 2<sup>48</sup>=281,474,976,710,656 subkeys. That's a lot
of subkeys (8 petabytes worth of 32-byte keys), but at scale and over a
key's lifetime the risk may become non-negligible.

AES-KW is defined in [RFC
3394](https://datatracker.ietf.org/doc/html/rfc3394), and is
standardized in [NIST SP 800 38F](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-38F.pdf).

To integrate AES-KW, we recommend the package
[go-aes-key-wrap](https://pkg.go.dev/github.com/nickball/go-aes-key-wrap).

---

#### Checksums potential information leak

The specification writes that "Each time a chunk is produced, a
checksum of the data is computed for internal purposes and recording
within the snapshot itself."

The checksum is a SHA-256 hash of the cleartext data. A MAC of the
checksum is then used as blob ID, although the checksum seems to be [used
as an
index](https://github.com/PlakarKorp/plakar/blob/506a90863ce5ef1c70a8e42cb52a8b791eec3c65/repository/repository.go#L329).

Our main observation is that the knowledge of a checksum (as hash of
cleartext data) can allow an attacker to identify if a given piece of
cleartext data is stored. Depending on the threat model, this may or may
not be an issue.

> note from the developers:\
> We forgot to explain that digests were not visible within a backup repository as they were only part of encrypted indexes.
> They were supposedly only available locally to the software after it had fetched and decrypted the repository state.
> Regardless,
> we figured a way to rework our lookups and adapted the codebase to work fully on MAC and no longer make use of digests,
> removing all potential concerns over digests.

Furthermore, we suggest to adjust terminology to avoid
misunderstandings and use the most accurate term:

- "Checksums" are generally defined as non-secure hash values designed
  to detect accidental errors (such as CRCs). In contrast, "hash values"
  or "digests", or "fingerprints" are generally created using
  cryptographic hash functoins, secure against adversarial changes.

- The function `ChecksumHMAC()` data is used to produce an
  `objects.Checksum` data. Here, we suggest to replated `ChecksumHMAC`
  with (for example) `MAC()` or `ComputeMAC`, as 1) HMAC is just a type
  of MAC (or PRF), like AES is a type of block cipher, and 2) the value computed
  is not a checksum, but also called MAC (message authentication code).

---

### Code

The proposed implementation in symmetric.go and hashing.go use reliable,
Go-native implementations of cryptographic components. It uses them in a
safe way, for example using strong randomness, properly initializing a
nonce/IV, and so on.

We just have a minor observation:


#### Potential deadlock

If the reader passed to `DecryptStream()` does not provide full chunks
of data, the read operations in the goroutine could stall indefinitely.
Unless the risk is really negligible, we recommend implementing a
timeout to prevent denials of service.

> note from the developers:\
> This comment prompted a review,
> our assessment is that our implementation will raise an error and cause DecryptStream() to fail on incomplete chunks of data.

---

## Remediation review

After discussion with the Plakar maintainers, we reviewed the changes
performed in the documentation and code to address our recommendations,
namely the following schemes as new defaults:

- Use of BLAKE3 for hashing and MAC
- Use of AES-GCM-SIV for chunk encryption
- Use of AES-KW for subkey encryption

---
_Updated doc:_
[CRYPTOGRAPHY.md](https://github.com/PlakarKorp/plakar/blob/main/CRYPTOGRAPHY.md) 

In
[CRYPTOGRAPHY.md#current-defaults](https://github.com/PlakarKorp/plakar/blob/main/CRYPTOGRAPHY.md#current-defaults),
nit:

- KEYED BLAKE3 -> Keyed BLAKE3
- ARGON2ID -> Argon2id

(Also in the code, s/Argon2ID/Argon2id)

--- 
_Updated symmetric.go_:
[/encryption/symmetric.go](https://github.com/PlakarKorp/plakar/blob/main/encryption/symmetric.go)

No problem found.

--- 
**Switch to ARGON2ID:**
>
> [PR #447](https://github.com/PlakarKorp/plakar/pull/447/files): replaces the default KDF and allows plugging SCRYPT or PBKDF2 if required (not exposed)
>

The Argon2id parameters seem to be 256KB only, is that intended? I'd
recommend 256MB or more.

```
Argon2IDParams: &Argon2IDParams{
        SaltSize: saltSize,
        Time:     4,
        Memory:   256 * 1024,
        Threads:  uint8(runtime.NumCPU()),
        KeyLen:   32,
},
```


> note from the developers:\
> Confusingly, the size is not expressed in bytes but in kilo-bytes as confirmed by the documentation at [golang.org/x/crypto/argon2](https://pkg.go.dev/golang.org/x/crypto/argon2):
> "The time parameter specifies the number of passes over the memory and the memory parameter specifies the size of the memory in KiB. For example memory=64*1024 sets the memory cost to ~64 MB."
>
> The Threads parameter was also lowered to 1 in a following commit after approval by the auditor.

---
**Switch to BLAKE3:**
>
> [PR #448](https://github.com/PlakarKorp/plakar/pull/448/files): Allows using BLAKE3 as a hasher for our HMAC function,
> we switched to BLAKE3 by default instead of SHA256 in a separate commit.
> [PR #457](https://github.com/PlakarKorp/plakar/pull/457/files) described below effectively unplugs all digests to only compute HMAC.
>

OK (where the B3 HMAC is replaced by keyed B3 in another PR)

---
**Switch to AES256-KW:**
>
> [PR #455](https://github.com/PlakarKorp/plakar/pull/455/files): split data encryption and subkey encryption, allow using AES256-KW
> 

On the verification canary: AES-KW includes an integrity check, to
ensure that the unwrapped key (a subkey decrypted using the
passphrase-derived key) is correct. However keeping the passphrase
verification canary is fine, and needed when AES-KW is not the subkey
encryption scheme used.

---
**Switch to AES256-GCM-SIV:**
> 
> [PR #465](https://github.com/PlakarKorp/plakar/pull/465/files): switch to AES256-GCM-SIV #465
> 

Looks good, no problem found, OK with the tink package usage.

--- 
**Switch from digests to MAC**
>
> [PR #457](https://github.com/PlakarKorp/plakar/pull/457/files): kill checksums use hmac only.
> No more calls to Checksum(), function was removed and we now only rely on ComputeMAC().
> The command `plakar digest` allows to compute a digest instead of a MAC if needed,
> it no longer resort to digests recorded in the snapshot.
>
> [PR #469](https://github.com/PlakarKorp/plakar/pull/469/files): the type objects.Checksum was renamed to objects.MAC,
> and mecanical change to rename all types and variables for consistency.
>

Looks good, no problem found.

---
**Switch from HMAC-BLAKE3 to Keyed BLAKE3**
>
> [PR #484](https://github.com/PlakarKorp/plakar/pull/484/files): Switch from HMAC-BLAKE3 to Keyed BLAKE3
>

Looks good, no problem found.
