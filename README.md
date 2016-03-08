# Blobs

[![Build Status](https://travis-ci.org/tanmaykm/Blobs.jl.svg?branch=master)](https://travis-ci.org/tanmaykm/Blobs.jl)


## Terminology
### Blob
A blob of data, not necessarily materialized in RAM at all times. When not in RAM, the data is backed into some kind of persistent storage,
and only metadata to retrieve it is available in the blob data structure. Locality of a blob is determined by the accessibility of the
backed up data. Blobs can be serialized (only metadata gets serialized), and can be accessed at a different time or location as long as the 
locality allows. Each blob is identified by an id (a UUID).

Metadata (that defines source/sink) of a blob can be:
- File segment
    - identified by filename, offset and size
    - optionally memory mapped
    - can be local/HDFS/S3
- Function
    - function and parameters
    - a way to initialize blobs (possibly from other blobs)
- More than one of the above
    - when there are alternate ways
    - choice of reader/writer decides the metadata used

Memory of materialized blobs are held with weak references to allow them to be garbage collected on memory pressure. To prevent the
working set from being garbage collected, a separate LRU cache with configurable limits holds strong references to the working set.
Eviction from the LRU cache invokes the configured writer to save changes to the blob data.

### Blob Collection
Is a list of related blobs. The whole of which blobs are part. It defines common properties for all contained blobs:
- maximum size
- mutability and mutability properties
- reader and writer types
- node map of the participating nodes

Blob collections have unique ids (UUID). Registering a blob collection makes it possible to access the collection and blobs in it with 
just their ids. It can be registered at all worker processes where it will be needed.

A Blob collection can be serialized/deserialized on its own. But in practice it will be more useful to deal with a higher type that makes
use of blob collections and provides more meaning to the data. Thus, a blob collection is really a low level concept.

### Node Map
A node map holds a mapping of the process to its actual pid, and IP address / hostname of the physical machine it is currently running on.
A pid can be running on a host with multiple IP addresses and hostnames. More than one pids can be running on a host.
This is used to map localities to nodes. The blob framework also uses this information for data movement.

### Mutability
Blobs can be mutable. They can be written or appended to. Since blobs have a maximum size limit, writing beyond the size fails with a spill.
The spill callback is called at that point, which either throws an exception, or allocates a new blob. Spill callbacks are registered by
blob groups. (Note: handling of spills is not implemented yet)

### Locality
Blobs can be located at one or more processes, or physical nodes. Locality is determined by the blob reader/writer implementation.
Locality can be:
- Strong
    - are present only locally
    - accessing these blobs from a non local process entail a call to a remote Julia process
    - may go away if the host/process holding it fails
- Weak
    - can be accessed from anywhere (e.g. HDFS, S3, NFS)
    - but may be preferably on some nodes (e.g. HDFS replication)
    - typically not affected by single host/process failures

### Blob IO
Provide `read` and `write` methods for blobs. 
    - implementations: FS, HDFS, functions
    - more can be plugged in from outside


## Usage

The easiest way to start using blobs is through `ProcessGlobalBlob`.

E.g. to create a blob store that stores blobs up to 100MB in memory and offloads anything more to the disk:

````julia
blobstore = ProcessGlobalBlob(100*1024*1024, maxmem)
````

Here, `maxmem` is the caching strategy and 100MB is the maximum limit. The other possible strategy is `maxcount` with the number of blobs to cache as the limit.

````julia
# to store something:
token = append!(blobstore, rand(10^6))

# to retrieve it back, use the token:
A = load(blobstore, token)

# to remove it from the store:
flush(blobstore, token)
````

Using blob collections and blobs directly allows access to more functionalities.

### Blob Collection

To create a blob collection, we need to provide the type of the blob it stores and specify ways to read and write them:

````julia
# read and write methods (using the provided FileBlobIO here)
io = FileBlobIO(Matrix{Float64}, true)

# we want our blobs saved when changed (also indicating the max blob size)
mut = Mutable(128 * 1024 * 1024, io)

# create the collection, passing the writer (mutability type) and the reader (io)
coll = BlobCollection(Matrix{Float64}, mut, io)
````

Append to a blob collection:

````julia
# append a matrix of `Float64` stored in the first 128MB of file named `blob1`.
blob = append!(coll, Matrix{Float64}, FileMeta("blob1", 0, 128*1024*1024), StrongLocality(myid()))

# or append a new matrix of `Float64`, provided via `A`, and store it to `blob1` when needed.
blob = append!(coll, Matrix{Float64}, FileMeta("blob1", 0, 128*1024*1024), StrongLocality(myid()), Nullable(A))
````

Registring a blob collection on a remote process makes it's blobs accessibe from there:
````julia
# register `coll` at all remote processes
register(coll, workers())
````

Blobs and collections can be accessed with their ids: `blob.id` or `coll.id`. To load a blob from the collection:
````julia
load(coll, blobid)
````

The complete list of APIs are not documented yet. It is best to take a look at the [examples](examples) and [tests](test) to get more information on how to use this.

