module DenseArrayBlobs

using Blobs
import Blobs: @logmsg, load, save
using Base.Random: UUID
import Base: serialize, deserialize, getindex, setindex!, size

export DenseMatBlobs, size, getindex, setindex!, serialize, deserialize

const BYTES_128MB = 128 * 1024 * 1024

def_sz{T}(::Type{T}, blk_sz::Int=BYTES_128MB) = floor(Int, blk_sz / sizeof(T))

# D = dimension that is split
# N = value of the other dimension, which is constant across all splits
type DenseMatBlobs{T,D,N} <: AbstractMatrix{T}
    metadir::AbstractString
    sz::Tuple
    splits::Vector{Pair}        # keep a mapping of index ranges to blobs
    coll::BlobCollection{Vector{T}}
end

size(dm::DenseMatBlobs) = dm.sz

split_ranges(dm::DenseMatBlobs) = [p.first for p in dm.splits]

function splitidx(dm::DenseMatBlobs, splitdim_idx::Int)
    for splitnum in 1:length(dm.splits)
        p = dm.splits[splitnum]
        range = p.first
        if splitdim_idx in range
            return splitnum
        end
    end
    throw(BoundsError("DenseMatBlobs $(dm.sz) split on dimension $D", splitdim_idx))
end

function load{T,D,N}(dm::DenseMatBlobs{T,D,N}, splitdim_idx::Int)
    splitnum = splitidx(dm, splitdim_idx)
    p = dm.splits[splitnum]
    range = p.first
    bid = p.second
    bytes = load(dm.coll, bid)
    M = length(range)
    split_dim = (D == 1) ? (M,N) : (N,M)
    #@logmsg("loaded split idx:$(splitdim_idx) from splitnum $splitnum with range: $range")
    reshape(bytes, split_dim), range
end

function relidx(range::Range, D::Int, i1::Int, i2::Int)
    if D == 1
        (i1 - first(range) + 1), i2
    else
        i1, (i2 - first(range) + 1)
    end
end

getindex{T,D,N}(dm::DenseMatBlobs{T,D,N}, i::Int) = getindex(dm, ind2sub(size(dm), i)...)

function getindex{T,D,N}(dm::DenseMatBlobs{T,D,N}, i1::Int, i2::Int)
    splitdim_idx = (D == 1) ? i1 : i2
    part, range = load(dm, splitdim_idx)
    part[relidx(range, D, i1, i2)...]
end

function getindex{T,N}(dm::DenseMatBlobs{T,1,N}, i1::Int, ::Colon)
    part, range = load(dm, i1)
    reli1, reli2 = relidx(range, 1, i1, 1)
    part[reli1,:]
end

function getindex{T,N}(dm::DenseMatBlobs{T,2,N}, ::Colon, i2::Int)
    part, range = load(dm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2]
end

setindex!{T,D,N}(dm::DenseMatBlobs{T,D,N}, v::T, i::Int) = setindex!(dm, v, ind2sub(size(dm), i)...)

function setindex!{T,D,N}(dm::DenseMatBlobs{T,D,N}, v::T, i1::Int, i2::Int)
    splitdim_idx = (D == 1) ? i1 : i2
    part, range = load(dm, splitdim_idx)
    part[relidx(range, D, i1, i2)...] = v
end

function setindex!{T,N}(dm::DenseMatBlobs{T,1,N}, v, i1::Int, ::Colon)
    part, range = load(dm, i1)
    reli1, reli2 = relidx(range, 1, i1, 1)
    part[reli1,:] = v
end

function setindex!{T,N}(dm::DenseMatBlobs{T,2,N}, v, ::Colon, i2::Int)
    part, range = load(dm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2] = v
end

function serialize(s::SerializationState, dm::DenseMatBlobs)
    Serializer.serialize_type(s, typeof(dm))
    serialize(s, dm.sz)
    serialize(s, dm.splits)

    coll = dm.coll
    serialize(s, coll.id)
    serialize(s, coll.mutability)
    serialize(s, coll.reader)
    serialize(s, coll.blobs)
    serialize(s, coll.maxcache)
end

function deserialize{T,D,N}(s::SerializationState, ::Type{DenseMatBlobs{T,D,N}})
    VT = Vector{T}
    sz = deserialize(s)
    splits = deserialize(s)

    coll_id = deserialize(s)
    coll_mut = deserialize(s)
    coll_reader = deserialize(s)
    coll_blobs = deserialize(s)
    coll_maxcache = deserialize(s)

    coll = BlobCollection(VT, coll_mut, coll_reader; maxcache=coll_maxcache, id=coll_id)
    coll.blobs = coll_blobs
    DenseMatBlobs{T,D,N}("", sz, splits, coll)
end

function DenseMatBlobs{T}(::Type{T}, splitdim::Int, sz::Tuple, metadir::AbstractString, max_size::Int=def_sz(T))
    VT = Vector{T}
    mut = Mutable(max_size, FileBlobIO(VT, true))
    coll = BlobCollection(VT, mut, FileBlobIO(VT, true))

    @logmsg("creating new dense mat blobs...")
    # create new
    splits = Pair[]
    N = sz[(splitdim == 1) ? 2 : 1]
    M = sz[splitdim]
    deltaM = max(1, floor(Int, max_size/N))
    startidx = 1
    while startidx <= sz[splitdim]
        idxrange = startidx:min(M, startidx + deltaM)
        meta = FileMeta(joinpath(metadir, string(length(splits)+1)), 0, length(idxrange)*N*sizeof(T))
        blob = append!(coll, VT, meta, StrongLocality(myid()))
        push!(splits, idxrange => blob.id)
        startidx = last(idxrange) + 1
        @logmsg("created blob for range $idxrange")
    end

    # load all blobs to initialize
    idx = 1
    for idx in 1:length(splits)
        p = splits[idx]
        blobid = p.second
        part = load(coll, blobid)
        fill!(part, idx)
        @logmsg("initialized $blobid with $idx")
    end
    @logmsg("saving all blobs")
    save(coll)

    @logmsg("saving densematarray")
    mat = DenseMatBlobs{T,splitdim,N}(metadir, sz, splits, coll)
    open(joinpath(metadir, "meta"), "w") do io
        serialize(SerializationState(io), mat)
    end
    mat
end

function DenseMatBlobs(metadir::AbstractString)
    @logmsg("reading back densemataray from $metadir")
    open(joinpath(metadir, "meta"), "r") do io
        deserialize(SerializationState(io))
    end
end

end # module
