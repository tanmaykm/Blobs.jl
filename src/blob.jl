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

load{T}(meta::TypedMeta, reader::BlobIO{T}) = load(meta.metadict[typeof(reader)], reader)
save{T}(databytes::T, meta::TypedMeta, writer::BlobIO{T}) = save(databytes, meta.metadict[typeof(writer)], writer)

# file blob io
# files have strong locality to the machine
function locality{T}(::Type{FileBlobIO{T}}, nodemap::NodeMap=DEF_NODE_MAP)
    nodes, ips, hns = localto(myid(), nodemap)
    StrongLocality(nodes..., ips..., hns...)
end

function load{T<:Real}(meta::FileMeta, reader::FileBlobIO{Vector{T}})
    sz = floor(Int, meta.size / sizeof(T))
    if reader.use_mmap
        return Mmap.mmap(meta.filename, Vector{T}, (sz,), meta.offset)
    else
        open(meta.filename) do f
            seek(f, meta.offset)
            databytes = Array(T, sz)
            read!(f, databytes) 
            return databytes
        end
    end
end

function save{T<:Real}(databytes::Vector{T}, meta::FileMeta, writer::FileBlobIO{Vector{T}})
    if writer.use_mmap
        sync!(databytes, Base.MS_SYNC | Base.MS_INVALIDATE)
    else
        touch(meta.filename)
        open(meta.filename, "r+") do f
            seek(f, meta.offset)
            #@logmsg("writing $(typeof(databytes)) array of length $(length(databytes)), size $(sizeof(databytes)). expected: $(meta.size)")
            (sizeof(databytes) == meta.size) || throw(ArgumentError("Blob data not of expected size. Got $(sizeof(databytes)), expected $(meta.size)."))
            write(f, databytes)
        end
    end
    nothing
end

# function blob io
# function outputs have strong locality only to the pid
locality{T}(::Type{FunctionBlobIO{T}}, nodemap::NodeMap=DEF_NODE_MAP) = StrongLocality(myid())

load{T<:Real}(meta::FunctionMeta, fnio::FunctionBlobIO{Vector{T}}) = fnio.reader(meta.params...)
function save{T<:Real}(databytes::Vector{T}, meta::FunctionMeta, fnio::FunctionBlobIO{Vector{T}})
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
type BlobCollection{T, M<:Mutability}
    id::UUID
    mutability::M
    reader::BlobIO{T}
    nodemap::NodeMap
    blobs::Dict{UUID,Blob}
    maxcache::Int
    cache::LRU{UUID,T}
end

function BlobCollection{T,M<:Mutability}(::Type{T}, mutability::M, reader::BlobIO{T}; maxcache::Int=10, nodemap::NodeMap=DEF_NODE_MAP, id::UUID=uuid4())
    L = typeof(locality(reader))
    blobs = Dict{UUID,Blob{T,L}}()
    cache = LRU{UUID,T}(maxcache)
    coll = BlobCollection{T,M}(id, mutability, reader, nodemap, blobs, maxcache, cache)
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

function append!{T,L}(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, ::Type{L}, v::Nullable{T}=Nullable{T}())
    blob = Blob(T, L, blobmeta)
    isnull(v) || (blob.data.value = get(v))
    append!(coll, blob)
end
function append!{T}(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, loc::Locality, v::Nullable{T}=Nullable{T}())
    blob = Blob(T, blobmeta, loc)
    isnull(v) || (blob.data.value = get(v))
    append!(coll, blob)
end
append!(coll::UUID, blob::Blob) = append!(BlobCollection(id::UUID), blob)
function append!(coll::BlobCollection, blob::Blob)
    (blob.data.value == nothing) || (coll.cache[blob.id] = blob.data.value)
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

flush(coll::BlobCollection, wrkrs::Vector{Int}; callback::Bool=true) = flush(coll.id, wrkrs; callback=callback)
function flush(collid::UUID, wrkrs::Vector{Int}; callback::Bool=true)
    @sync for w in wrkrs
        @async remotecall_wait((collid)->flush(collid; callback=callback), w, collid)
    end
end
flush(collid::UUID; callback::Bool=true) = flush(BlobCollection(collid); callback=callback)
function flush(coll::BlobCollection; callback::Bool=true)
    for blob in values(coll.blobs)
        flush(coll, blob; callback=callback)
    end
end


# mutability of a blob collection may be switched at run time, whithout affecting anything else.
as_mutable{T,M<:Mutable}(coll::BlobCollection{T,M}, mutability::Mutable) = coll
as_immutable{T,M<:Immutable}(coll::BlobCollection{T,M}) = coll

as_immutable{T,M<:Mutable}(coll::BlobCollection{T,M}) = BlobCollection{T,Immutable}(coll.id, Immutable(), coll.reader, coll.nodemap, coll.blobs, coll.cache)
as_mutable{T,M<:Immutable}(coll::BlobCollection{T,M}, mutability::Mutable) = BlobCollection{T,Immutable}(coll.id, mutability, coll.reader, coll.nodemap, coll.blobs, coll.cache)

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
function load_local{T}(coll::BlobCollection{T}, blob::Blob{T})
    if !haskey(coll.cache, blob.id)
        val = blob.data.value
        (val == nothing) && (val = load(blob.metadata, coll.reader))
        blob.data.value = coll.cache[blob.id] = val
        blob.locality = locality(coll.reader, coll.nodemap)
    end
    (coll.cache[blob.id])::T
end

load(collid::UUID, blobid::UUID) = load(BlobCollection(collid), blobid)
load(coll::BlobCollection, blobid::UUID) = load(coll, coll.blobs[blobid])
load{T,L<:WeakLocality}(coll::BlobCollection{T}, blob::Blob{T,L}) = load_local(coll, blob)
function load{T,L<:StrongLocality}(coll::BlobCollection{T}, blob::Blob{T,L})
    if !haskey(coll.cache, blob.id)
        val = blob.data.value
        if val == nothing
            # select a node from blob's local nodes
            fetchfrom = select_local(coll, blob)
            val = remotecall_fetch(load_local, fetchfrom, coll.id, blob.id)
        end
        blob.data.value = coll.cache[blob.id] = val
    end
    (coll.cache[blob.id])::T
end

save(collid::UUID, blobid::UUID) = save(BlobCollection(collid), blobid)
save{T,M<:Mutable}(coll::BlobCollection{T,M}, blobid::UUID) = save(coll, coll.blobs[blobid])
function save{T,M<:Mutable}(coll::BlobCollection{T,M}, blob::Blob)
    if haskey(coll.cache, blob.id)
        save(blob.data.value, blob.metadata, coll.mutability.writer)
    end
end

flush(collid::UUID, blobid::UUID; callback::Bool=true) = flush(BlobCollection(collid), blobid; callback=callback)
flush{T,M<:Mutable}(coll::BlobCollection{T,M}, blobid::UUID; callback::Bool=true) = flush(coll, coll.blobs[blobid]; callback=callback)
function flush{T,M<:Mutable}(coll::BlobCollection{T,M}, blob::Blob; callback::Bool=true)
    if haskey(coll.cache, blob.id)
        delete!(coll.cache, blob.id; callback=callback)
    end
end

save{T,M<:Immutable}(coll::BlobCollection{T,M}, blobid::UUID) = nothing
save{T,M<:Immutable}(coll::BlobCollection{T,M}, blob::Blob) = nothing
