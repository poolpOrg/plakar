# cryptography design documentation

## Notes

1- Whenever it is written that data was randomly-generated, it is using the `crypto/rand` PRNG as its source of randomness.
2- The passphrase that protects a repository is the single most important secret: it must be safely backed up as loss means the repository data can't be recovered and compromission means the repository data is fully readable.
3- Repository can be stored in a public cloud, configuration and encrypted content being available to the hosting company and its employees


## Initialization of an encrypted repository

User creating the repository is prompted for a passphrase.

A 256-bits master encryption key is derived from the supplied passphrase using `scrypt` with a 128-bits randomly-generated salt and parameters N=32768, r=8, p=1 as suggested by the official package documentation (and double-checked through Google and OpenAI's ChatGPT).

A random 32-bytes block is also generated and encrypted with the master key using the method described at the end of the document.

The scrypt parameters, the salt and the encrypted block are then stored in the repository configuration file which remains cleartext as it needs to let clients determine how they will work with that repository.

```
configuration = {
    KDF="scrypt",
    N=32768,
    r=8,
    p=1,
    salt=[]byte("....")     // random bytes
    canary=[]byte("...")    // encrypted random bytes
}
```

## Opening of an encrypted repository

1- The client retrieves the repository configuration and obtains the scrypt parameters, the salt as well as the encrypted random 32-bytes array.

2- It prompts user for a passphrase and derives the master key using the scrypt parameters and salt.

3- It verifies that the derived master key successfully decrypts the canary block using GCM integrity check.

4- If decryption works, master key will be used during the session, otherwise the client errors because repository passphrase mismatches.



## Internal structure of a repository

A repository structure can be viewed as a key-value store, keeping blobs of data of varying sizes into packfiles and an index to locate which packfile has which blob at which offset and with which length. The key to lookups is the `sha256` sum of a blob **_prior to encryption_** as it is necessary to allow deduplication. While a `sha256` should not reveal the content of its input, the indexes are encrypted so that these "cleartext" checksums are not readily available to people without the passphrase.

**The encryption function will be described in a few sections, it can be assumed that anything described as encrypted has gone through the same function taking the master key and a cleartext input and producing the encrypted output.**


### Index structure
The index structure is fairly simple, it maps a blob type and cleartext checksum to a packfile, offset and length.

```
[blob type][blob checksum][packfile checksum][offset][length]
[blob type][blob checksum][packfile checksum][offset][length]
[blob type][blob checksum][packfile checksum][offset][length]
```

It allows performing the following lookup:

```
lookup(type, checksum) -> packfile, offset, length
```

The index is split into "delta" files which are aggregated by the client to rebuild a local view.
**Each delta file is encrypted as a whole**, the client will decrypt them and parse the records to insert in a local cache.
When creating a backup, **a client will issue a new encrypted delta file** that will be stored in the repository.


### Packfile structure
The packfile structure is also fairly simple, it contains a sequence of encrypted blobs, an encrypted local index and an encrypted footer:

```
[encrypted blob]
[encrypted blob]
[encrypted blob]
[...]
[encrypted blob]
[encrypted blob]
[encrypted packfile index]
[encrypted packfile footer]
```

Each blob is encrypted individually so that they can be accessed and decrypted individually as long as their location and length is known.
The index and footer are also encrypted individually for the same reason.


#### Packfile index

The packfile index stores the following record for each blob:
```
[type][cleartext checksum][offset][length]
```

#### Packfile footer

The packfile footer stores a few metadata such as creation data, number of items, offset of the index and the checksum of the packfile index as a fast integrity check.


## Backup process

A snapshot header is created with a random 256-bits identifier.

As backup is performed, blobs of data are produced from various sources: file content, filesystem structure, plakar internal structures, ... and a checksum of the cleartext is computed and checked against the repository to verify if data needs to be pushed or if it already exists in a packfile there. If data is missing from the repository, it is compressed, encrypted and pushed with the cleartext checksum as a lookup key.

The snapshot header contains some of the cleartext checksums that are needed to rebuild a full view of the backup, and it operates as a "commit" so that a backup is visible to other clients only when a snapshot header is pushed to the repository. It is pushed last, with its identifier as a lookup key, after being compressed and encrypted itself, so the repository stores it in a packfile as well.


## Restore process

In a simplified view, a snapshot can be seen as a tree of blobs where each node is either a structure that provides checksums to lookup other nodes in packfiles or raw data. The restore process starts by looking up the snapshot header which provides the root of the tree, and from there the appropriate lookups are done to find the nodes that are necessary for a particular restore.

As the snapshot header and each node is encrypted, the restore process is essentially a loop of lookups and decryptions, some leading to mapping structures in memory and others to restoring data to a file.


## Encryption and decryption functions

At the exception of the above, all encryption and decryption in `plakar` takes place through the same two functions.

Encryption is performed using `AES256-GCM` with individual 256-bits subkeys randomly-generated and protected by the master key, so that in the event of one subkey leaking or being broken for any reason unrelated to the master key leaking, then not all data would be immediately at risk.


### Encryption
The encryption works as follows:

    1- an encryption function takes a master key and an input buffer
    2- it randomly generates a 256-bits subkey and a subkey nonce
    3- it AES25-GCM encrypts the input buffer with that subkey into an output buffer
    4- a random master key nonce is generated
    5- subkey and subkey nonce are AES256-GCM encrypted with master key and master key nonce into a subkey block prepended to output buffer

    [encrypted subkey block] [encrypted data block]
     ^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^
     master key encrypted     subkey encrypted


### Decryption
The decryption works as follows:

    1- a decryption function takes a master key and an input buffer
    2- the input buffer is split into two parts: the subkey block and the data block
    3- the subkey block is decrypted so that subkey and subkey nonce are retrieved, GCM integrity check validates master key
    4- the data block is decrypted with the subkey and subkey nonce, GCM integrity check validates subkey
