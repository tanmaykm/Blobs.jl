# process global blob collection.
const MAXBLOBSZ = 128*1024*1024

mutable struct ProcessGlobalBlob
    maxcache::Int
    strategy::Function
    storepath::String
    coll::BlobCollection

    function ProcessGlobalBlob(maxcache::Int, strategy::Function, storepath::String=tempdir())
        io = FileBlobIO(Any, true)
        coll = BlobCollection(Any, Mutable(MAXBLOBSZ, io), io; maxcache=maxcache, strategy=strategy)
        
        new(maxcache, strategy, storepath, coll)
    end
end

function append!(gb::ProcessGlobalBlob, data::T) where T
    meta = FileMeta("", 0, Base.summarysize(data))
    coll = gb.coll
    blob = append!(coll, T, meta, StrongLocality(myid()), Nullable(data))
    meta.filename = joinpath(gb.storepath, string(blob.id))
    @logmsg("keeping as blob $(meta.filename)")
    #@logmsg(data)
    (coll.id, blob.id)
end

load(gb::ProcessGlobalBlob, ids::Tuple) = load(ids...)
flush(gb::ProcessGlobalBlob, ids::Tuple) = flush(ids...)
#=
type ProcessGlobalBlobs
    maxcache::Int
    storepath::String
    collections::Dict{Type, BlobCollection}

    function ProcessGlobalBlobs(maxcache::Int, storepath::String=tempdir())
        new(maxcache, storepath, Dict{Type,BlobCollection}())
    end
end


getcoll(gb::ProcessGlobalBlobs, ::Type{T}) where T <: Any = _getcoll(gb, T)
getcoll(gb::ProcessGlobalBlobs, ::Type{T}) where T <: Array = getcoll(gb, T, eltype(T))
getcoll(gb::ProcessGlobalBlobs, ::Type{T}, ::Type{E}) where {T<:Array, E<:Real} = _getcoll(gb, Array{E})
getcoll(gb::ProcessGlobalBlobs, ::Type{T}, ::Type{E}) where {T<:Array,E<:Any} = _getcoll(gb, Any)

function _getcoll(gb::ProcessGlobalBlobs, ::Type{T}) where T
    allblobs = gb.collections
    if !(T in keys(allblobs))
        io = FileBlobIO(T, true)
        allblobs[T] = BlobCollection(T, Mutable(MAXBLOBSZ, io), io; maxcache=2)
    end
    allblobs[T]
end
storepath(gb::ProcessGlobalBlobs, T::Type) = joinpath(gb.storepath, string(hash(T)))

function append!(gb::ProcessGlobalBlobs, data::T) where T
    meta = FileMeta("", 0, sersz(data))
    coll = getcoll(T)
    blob = append!(coll, T, meta, StrongLocality(myid()), Nullable(data))
    meta.filename = joinpath(storepath(T), string(blob.id))
    @logmsg("keeping as blob $(meta.filename)")
    (coll.id, blob.id)
end
=#
