abstract BlobMeta

type FileMeta <: BlobMeta
    filename::AbstractString
    offset::Int
    size::Int
end

type FunctionMeta <: BlobMeta
    params::Tuple
end

# Typed meta holds multiple metadata for different reader types.
# Reader types have predefined mapping to metadata types
# TODO: rename MetaCollection
type TypedMeta <: BlobMeta
    metadict::Dict{Type,BlobMeta}
    function TypedMeta(m...)
        new(Dict{Type,BlobMeta}(m...))
    end
end

load{T<:BlobIO}(meta::TypedMeta, reader::T) = load(meta.metadict[T], reader)
save{T<:BlobIO}(databytes::Vector{UInt8}, meta::TypedMeta, writer::T) = save(databytes, meta.metadict[T], writer)

# file blob io
# files have strong locality to the machine
function locality(::Type{FileBlobIO}, nodemap::NodeMap=DEF_NODE_MAP)
    nodes, ips, hns = localto(myid(), nodemap)
    StrongLocality(nodes..., ips..., hns...)
end

function load(meta::FileMeta, reader::FileBlobIO)
    if reader.use_mmap
        return Mmap.mmap(meta.filename, Vector{UInt8}, (meta.size,), meta.offset)
    else
        open(meta.filename) do f
            seek(f, meta.offset)
            databytes = Array(UInt8, meta.size)
            read!(f, databytes) 
            return databytes
        end
    end
end

function save(databytes::Vector{UInt8}, meta::FileMeta, writer::FileBlobIO)
    if writer.use_mmap
        sync!(databytes, Base.MS_SYNC | Base.MS_INVALIDATE)
    else
        touch(meta.filename)
        open(meta.filename, "r+") do f
            seek(f, meta.offset)
            write(f, sub(databytes, 1:meta.size))
        end
    end
    nothing
end

# function blob io
# function outputs have strong locality only to the pid
locality(::Type{FunctionBlobIO}, nodemap::NodeMap=DEF_NODE_MAP) = StrongLocality(myid())

load(meta::FunctionMeta, fnio::FunctionBlobIO) = fnio.reader(meta.params...)
function save(databytes::Vector{UInt8}, meta::FunctionMeta, fnio::FunctionBlobIO)
    fnio.writer(databytes, meta.params...)
    nothing
end

# Blobs
type Blob{T,L}
    id::UUID
    metadata::BlobMeta
    locality::L
    data::WeakRef
end
Blob{T,L}(::Type{T}, metadata::BlobMeta, locality::L, id::UUID=uuid4()) = Blob{T,L}(id, metadata, locality, WeakRef())
Blob{T,L}(::Type{T}, ::Type{L}, metadata::BlobMeta, nodeid::Union{Int,Vector,Tuple}=myid(), id::UUID=uuid4()) = Blob{T,L}(id, metadata, L(nodeid...), WeakRef())

function serialize(s::SerializationState, blob::Blob)
    Serializer.serialize_type(s, typeof(blob))
    serialize(s, blob.id)
    serialize(s, blob.metadata)
    serialize(s, blob.locality)
end

function deserialize{T,L}(s::SerializationState, ::Type{Blob{T,L}})
    id = deserialize(s)
    metadata = deserialize(s)
    locality = deserialize(s)
    Blob{T,L}(id, metadata, locality, WeakRef())
end

islocal(blob::Blob) = islocal(blob, myid())
islocal(blob::Blob, nodeid::Int) = islocal(blob.locality, nodeid)

# Blob Collections
type BlobCollection{T, R<:BlobIO, M<:Mutability}
    id::UUID
    mutability::M
    reader::R
    nodemap::NodeMap
    blobs::Dict{UUID,Blob}
    maxcache::Int
    cache::LRU{UUID,Vector{T}}
end

function BlobCollection{T,R<:BlobIO,M<:Mutability}(::Type{T}, mutability::M, reader::R; maxcache::Int=10, nodemap::NodeMap=DEF_NODE_MAP, id::UUID=uuid4())
    L = typeof(locality(reader))
    blobs = Dict{UUID,Blob{T,L}}()
    cache = LRU{UUID,Vector{T}}(maxcache)
    coll = BlobCollection{T,R,M}(id, mutability, reader, nodemap, blobs, maxcache, cache)
    coll.cache.cb = (blobid,data)->save(coll, blobid)
    register(coll)
    coll
end

BlobCollection(id::UUID) = BLOB_REGISTRY[id]

const BLOB_REGISTRY = Dict{UUID,BlobCollection}()
function register(coll::BlobCollection)
    @logmsg("registering coll $(coll.id) with $(length(coll.blobs)) blobs")
    BLOB_REGISTRY[coll.id] = coll
    nothing
end
function register(coll::BlobCollection, wrkrs::Vector{Int})
    @sync for w in wrkrs
        @async remotecall_wait(register, w, coll)
    end
end

deregister(coll::BlobCollection) = deregister(coll.id)
deregister(coll::UUID) = delete!(BLOB_REGISTRY, coll)
function deregister(coll::BlobCollection, wrkrs::Vector{Int})
    @sync for w in wrkrs
        @async remotecall_wait(deregister, w, coll)
    end
end

append!{T,L}(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, ::Type{L}) = append!(coll, Blob(T, L, blobmeta))
append!{T}(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, loc::Locality) = append!(coll, Blob(T, blobmeta, loc))
append!(coll::UUID, blob::Blob) = append!(BlobCollection(id::UUID), blob)
function append!(coll::BlobCollection, blob::Blob)
    coll.blobs[blob.id] = blob
end
blobids(coll::BlobCollection) = keys(coll.blobs)

# Load and save a blob collection
# Save just stores the list of blobs and their metadata.
# When loaded in a different environment, the mutability and reader types, and the nodemap may be different.
# Load needs to be provided with a compatible BlobCollection to read the blob contents.
function load(coll::BlobCollection, filename::AbstractString)
    open(filename) do f
        load(coll, f)
    end
end
function load(coll::BlobCollection, io::IO)
    blobs = deserialize(io)
    for blob in values(blobs)
        coll.blobs[blob.id] = blob
    end
    coll
end
save(collid::UUID, io::IO, wrkrs::Vector{Int}) = save(BlobCollection(collid), io, wrkrs)
function save(coll::BlobCollection, filename::AbstractString, wrkrs::Vector{Int})
    open(filename, "w") do f
        save(coll, f, wrkrs)
    end
end
function save(coll::BlobCollection, io::IO, wrkrs::Vector{Int})
    save(coll, wrkrs)
    serialize(SerializationState(io), coll.blobs)
    coll
end
save(coll::BlobCollection, wrkrs::Vector{Int}) = save(coll.id, wrkrs)
function save(collid::UUID, wrkrs::Vector{Int})
    @sync for w in wrkrs
        @async remotecall_wait(save, w, collid)
    end
end
save(collid::UUID) = save(BlobCollection(collid))
function save(coll::BlobCollection)
    for blob in values(coll.blobs)
        save(coll, blob)
    end
end


# mutability of a blob collection may be switched at run time, whithout affecting anything else.
as_mutable{T,R,M<:Mutable}(coll::BlobCollection{T,R,M}, mutability::Mutable) = coll
as_immutable{T,R,M<:Immutable}(coll::BlobCollection{T,R,M}) = coll

as_immutable{T,R,M<:Mutable}(coll::BlobCollection{T,R,M}) = BlobCollection{T,R,Immutable}(coll.id, Immutable(), coll.reader, coll.nodemap, coll.blobs, coll.cache)
as_mutable{T,R,M<:Immutable}(coll::BlobCollection{T,R,M}, mutability::Mutable) = BlobCollection{T,R,Immutable}(coll.id, mutability, coll.reader, coll.nodemap, coll.blobs, coll.cache)

# select a node local to the blob to fetch blob contents from
select_local(coll::BlobCollection, blobid::UUID) = select_local(coll, coll.blobs[blobid])
function select_local(coll::BlobCollection, blob::Blob)
    locality = blob.locality
    islocal(locality, myid()) && (return myid())
    shuffle(collect(locality.nodes))[1]
end

function load_local(collid::UUID, blob::UUID)
    coll = BlobCollection(collid)
    load_local(coll, coll.blobs[blob])
end
function load_local{T,R}(coll::BlobCollection{T,R}, blob::Blob{T})
    if !haskey(coll.cache, blob.id)
        val = blob.data.value
        (val == nothing) && (val = load(blob.metadata, coll.reader))
        blob.data.value = coll.cache[blob.id] = reinterpret(T, val)::Vector{T}
        blob.locality = locality(R, coll.nodemap)
    end
    (coll.cache[blob.id])::Vector{T}
end

load(collid::UUID, blobid::UUID) = load(BlobCollection(collid), blobid)
load(coll::BlobCollection, blobid::UUID) = load(coll, coll.blobs[blobid])
load{T,R,L<:WeakLocality}(coll::BlobCollection{T,R}, blob::Blob{T,L}) = load_local(coll, blob)
function load{T,R,L<:StrongLocality}(coll::BlobCollection{T,R}, blob::Blob{T,L})
    if !haskey(coll.cache, blob.id)
        val = blob.data.value
        if val == nothing
            # select a node from blob's local nodes
            fetchfrom = select_local(coll, blob)
            val = remotecall_fetch(load_local, fetchfrom, coll.id, blob.id)
        end
        blob.data.value = coll.cache[blob.id] = reinterpret(T, val)::Vector{T}
    end
    (coll.cache[blob.id])::Vector{T}
end

save(collid::UUID, blobid::UUID) = save(BlobCollection(collid), blobid)
save{T,R,M<:Mutable}(coll::BlobCollection{T,R,M}, blobid::UUID) = save(coll, coll.blobs[blobid])
function save{T,R,M<:Mutable}(coll::BlobCollection{T,R,M}, blob::Blob)
    if haskey(coll.cache, blob.id)
        save(reinterpret(UInt8, blob.data.value), blob.metadata, coll.mutability.writer)
    end
end

save{T,R,M<:Immutable}(coll::BlobCollection{T,R,M}, blobid::UUID) = nothing
save{T,R,M<:Immutable}(coll::BlobCollection{T,R,M}, blob::Blob) = nothing
