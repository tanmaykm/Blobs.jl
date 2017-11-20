# mmap helper

ismmapped(a::Array) = (convert(UInt, pointer(a)) in mmapped)
delmmapped(a::Array) = delete!(mmapped, convert(UInt, pointer(a)))
syncmmapped(a::Array) = sync!(a, MS_SYNC | MS_INVALIDATE)
function blobmmap(file, ::Type{T}, dims, offset::Integer=Int64(0); grow::Bool=true, shared::Bool=true) where T <: Array
    a = Mmap.mmap(file, T, dims, offset; grow=grow, shared=shared)
    # Note: depends on https://github.com/JuliaLang/julia/pull/13995 for proper functioning
    finalizer(a, delmmapped)
    push!(mmapped, convert(UInt, pointer(a)))
    a
end

# blob meta
abstract type BlobMeta end

mutable struct FileMeta <: BlobMeta
    filename::String
    offset::Int
    size::Int
end

mutable struct FunctionMeta <: BlobMeta
    params::Tuple
end

# Typed meta holds multiple metadata for different reader types.
# Reader types have predefined mapping to metadata types
# TODO: rename MetaCollection
mutable struct TypedMeta <: BlobMeta
    metadict::Dict{Type,BlobMeta}
    function TypedMeta(m...)
        new(Dict{Type,BlobMeta}(m...))
    end
end

load(meta::TypedMeta, reader::BlobIO{T}) where T = load(meta.metadict[typeof(reader)], reader)
save(databytes::T, meta::TypedMeta, writer::BlobIO{T}) where T = save(databytes, meta.metadict[typeof(writer)], writer)

# file blob io
# files have strong locality to the machine
function locality(::Type{FileBlobIO{T}}, nodemap::NodeMap=DEF_NODE_MAP) where T
    nodes, ips, hns = localto(myid(), nodemap)
    StrongLocality(nodes..., ips..., hns...)
end

function load(meta::FileMeta, reader::FileBlobIO{Vector{T}}) where T <: Real
    @logmsg("load using FileBlobIO{Vector{$T}}")
    sz = floor(Int, meta.size / sizeof(T))
    if reader.use_mmap
        return blobmmap(meta.filename, Vector{T}, (sz,), meta.offset)
    else
        open(meta.filename) do f
            seek(f, meta.offset)
            databytes = Array{T}(sz)
            read!(f, databytes) 
            return databytes
        end
    end
end

function save(databytes::Vector{T}, meta::FileMeta, writer::FileBlobIO{Vector{T}}) where T <: Real
    @logmsg("save Vector{$T} using FileBlobIO{Vector{$T}}")
    dn = dirname(meta.filename)
    isdir(dn) || mkpath(dn)
    if writer.use_mmap && ismmapped(databytes)
        sync!(databytes, MS_SYNC | MS_INVALIDATE)
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

function load(meta::FileMeta, reader::FileBlobIO{Array{T}}) where T <: Real
    @logmsg("load using FileBlobIO{Array{$T}}")
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        pos1 = position(fhandle)

        hdrsz = read(fhandle, Int64)
        pos1 += sizeof(hdrsz)

        header = Array{Int64}(hdrsz)
        read!(fhandle, header)
        pos1 += sizeof(header)

        data = reader.use_mmap ? blobmmap(fhandle, Array{T,hdrsz}, tuple(header...), pos1) : read!(fhandle, Array{T}(header...))
        return data
    end
end

function save(M::Array{T}, meta::FileMeta, writer::FileBlobIO{Array{T}}) where T <: Real
    @logmsg("save Array{$T} using FileBlobIO{Array{$T}}")
    dn = dirname(meta.filename)
    isdir(dn) || mkpath(dn)
    if writer.use_mmap && ismmapped(M)
        syncmmapped(M)
    else
        header = Int64[size(M)...]
        hdrsz = Int64(length(header))

        touch(meta.filename)
        open(meta.filename, "r+") do fhandle
            seek(fhandle, meta.offset)
            write(fhandle, hdrsz)
            write(fhandle, header)
            write(fhandle, M)
        end
    end
    nothing
end

function load(meta::FileMeta, reader::FileBlobIO{Any})
    @logmsg("load using FileBlobIO{Any}")
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        return deserialize(SerializationState(fhandle))
    end
end

function save(data, meta::FileMeta, writer::FileBlobIO{Any})
    @logmsg("save $(typeof(data)) using FileBlobIO{Any}")
    dn = dirname(meta.filename)
    isdir(dn) || mkpath(dn)
    touch(meta.filename)
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        serialize(SerializationState(fhandle), data)
    end
end


# function blob io
# function outputs have strong locality only to the pid
locality(::Type{FunctionBlobIO{T}}, nodemap::NodeMap=DEF_NODE_MAP) where T = StrongLocality(myid())

load(meta::FunctionMeta, fnio::FunctionBlobIO{Vector{T}}) where T <: Real = fnio.reader(meta.params...)
function save(databytes::Vector{T}, meta::FunctionMeta, fnio::FunctionBlobIO{Vector{T}}) where T <: Real
    fnio.writer(databytes, meta.params...)
    nothing
end

# Blobs
mutable struct Blob{T,L}
    id::UUID
    metadata::BlobMeta
    locality::L
    data::WeakRef
end
Blob(::Type{T}, metadata::BlobMeta, locality::L, id::UUID=uuid4()) where {T, L} = Blob{T,L}(id, metadata, locality, WeakRef())
Blob(::Type{T}, ::Type{L}, metadata::BlobMeta, nodeid::Union{Int,Vector,Tuple}=myid(), id::UUID=uuid4()) where {T, L} = Blob{T,L}(id, metadata, L(nodeid...), WeakRef())

function serialize(s::SerializationState, blob::Blob)
    Serializer.serialize_type(s, typeof(blob))
    serialize(s, blob.id)
    serialize(s, blob.metadata)
    serialize(s, blob.locality)
end

function deserialize(s::SerializationState, ::Type{Blob{T,L}}) where {T, L}
    id = deserialize(s)
    metadata = deserialize(s)
    locality = deserialize(s)
    Blob{T,L}(id, metadata, locality, WeakRef())
end

islocal(blob::Blob) = islocal(blob, myid())
islocal(blob::Blob, nodeid::Int) = islocal(blob.locality, nodeid)

# Blob Collections
mutable struct BlobCollection{T, M<:Mutability}
    id::UUID
    mutability::M
    reader::BlobIO
    nodemap::NodeMap
    blobs::Dict{UUID,Blob}
    maxcache::Int
    cache::LRU{UUID,T}
end

function BlobCollection(::Type{T}, mutability::M, reader::BlobIO; maxcache::Int=10, strategy::Function=maxcount, nodemap::NodeMap=DEF_NODE_MAP, id::UUID=uuid4()) where {T, M<:Mutability}
    L = typeof(locality(reader))
    blobs = Dict{UUID,Blob{T,L}}()
    @logmsg("creating blobcollection with $maxcache $strategy")
    cache = LRU{UUID,T}(maxcache; strategy=strategy)
    coll = BlobCollection{T,M}(id, mutability, reader, nodemap, blobs, maxcache, cache)
    coll.cache.cb = (blobid,data)->save(coll, blobid)
    register(coll)
    coll
end

BlobCollection(id::UUID) = BLOB_REGISTRY[id]

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

max_cached(coll::BlobCollection) = coll.maxcache
function max_cached!(coll::BlobCollection, maxcache::Int)
    coll.maxcache = maxcache
    resize!(coll.cache, maxcache)
    nothing
end

function append!(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, ::Type{L}, v::Nullable{T}=Nullable{T}()) where {T, L}
    blob = Blob(T, L, blobmeta)
    isnull(v) || (blob.data.value = get(v))
    append!(coll, blob)
end
function append!(coll::Union{UUID,BlobCollection}, ::Type{T}, blobmeta::BlobMeta, loc::Locality, v::Nullable{T}=Nullable{T}()) where T
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
function load(coll::BlobCollection, filename::String)
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
function save(coll::BlobCollection, filename::String, wrkrs::Vector{Int})
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
as_mutable(coll::BlobCollection{T,M}, mutability::Mutable) where {T, M<:Mutable} = coll
as_immutable(coll::BlobCollection{T,M}) where {T, M<:Immutable} = coll

as_immutable(coll::BlobCollection{T,M}) where {T, M<:Mutable} = BlobCollection{T, Immutable}(coll.id, Immutable(), coll.reader, coll.nodemap, coll.blobs, coll.cache)
as_mutable(coll::BlobCollection{T,M}, mutability::Mutable) where {T, M<:Immutable} = BlobCollection{T, Immutable}(coll.id, mutability, coll.reader, coll.nodemap, coll.blobs, coll.cache)

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
function load_local(coll::BlobCollection, blob::Blob{T}) where T
    if !haskey(coll.cache, blob.id)
        val = load(blob.metadata, coll.reader)
        blob.data.value = coll.cache[blob.id] = val
        blob.locality = locality(coll.reader, coll.nodemap)
    end
    (coll.cache[blob.id])::T
end

load(collid::UUID, blobid::UUID) = load(BlobCollection(collid), blobid)
load(coll::BlobCollection, blobid::UUID) = load(coll, coll.blobs[blobid])
load(coll::BlobCollection, blob::Blob{T,L}) where {T, L<:WeakLocality} = load_local(coll, blob)
function load(coll::BlobCollection, blob::Blob{T,L}) where {T, L<:StrongLocality}
    if !haskey(coll.cache, blob.id)
        # select a node from blob's local nodes
        fetchfrom = select_local(coll, blob)
        val = (fetchfrom === myid()) ? load_local(coll.id, blob.id) : remotecall_fetch(load_local, fetchfrom, coll.id, blob.id)
        blob.data.value = coll.cache[blob.id] = val
    end
    (coll.cache[blob.id])::T
end

save(collid::UUID, blobid::UUID) = save(BlobCollection(collid), blobid)
save(coll::BlobCollection, blobid::UUID) = save(coll, coll.blobs[blobid])
function save(coll::BlobCollection{T,M}, blob::Blob) where {T,M<:Mutable}
    if haskey(coll.cache, blob.id)
        save(blob.data.value, blob.metadata, coll.mutability.writer)
    end
end

flush(collid::UUID, blobid::UUID; callback::Bool=true) = flush(BlobCollection(collid), blobid; callback=callback)
flush(coll::BlobCollection, blobid::UUID; callback::Bool=true) = flush(coll, coll.blobs[blobid]; callback=callback)
function flush(coll::BlobCollection, blob::Blob; callback::Bool=true)
    if haskey(coll.cache, blob.id)
        delete!(coll.cache, blob.id; callback=callback)
    end
end

save(coll::BlobCollection{T,M}, blobid::UUID) where {T, M<:Immutable} = nothing
save(coll::BlobCollection{T,M}, blob::Blob) where {T, M<:Immutable} = nothing
