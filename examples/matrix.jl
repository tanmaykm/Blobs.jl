module MatrixBlobs

using Blobs
import Blobs: @logmsg, load, save, flush, locality
using Base.Random: UUID
import Base: serialize, deserialize, getindex, setindex!, size, append!, flush, *

export DenseMatBlobs, SparseMatBlobs, size, getindex, setindex!, serialize, deserialize, save, load, flush, *

const BYTES_128MB = 128 * 1024 * 1024

def_sz(::Type{T}, blk_sz::Int=BYTES_128MB) where T = floor(Int, blk_sz / sizeof(T))

function relidx(range::Range, D::Int, i1::Int, i2::Int)
    if D == 1
        (i1 - first(range) + 1), i2
    else
        i1, (i2 - first(range) + 1)
    end
end

mutable struct SparseMatBlobs{Tv,Ti} <: AbstractMatrix{Tv}
    metadir::AbstractString
    sz::Tuple
    splits::Vector
    coll::BlobCollection{SparseMatrixCSC{Tv,Ti}}
end

# D = dimension that is split
# N = value of the other dimension, which is constant across all splits
mutable struct DenseMatBlobs{T,D,N} <: AbstractMatrix{T}
    metadir::AbstractString
    sz::Tuple
    splits::Vector        # keep a mapping of index ranges to blobs
    coll::BlobCollection{Matrix{T}}
end

const MatBlobs = Union{SparseMatBlobs,DenseMatBlobs}


locality(dm::MatBlobs) = locality(dm.coll.reader)

size(dm::MatBlobs) = dm.sz

split_ranges(dm::MatBlobs) = [p.first for p in dm.splits]

function splitidx(dm::MatBlobs, splitdim_idx::Int)
    for splitnum in 1:length(dm.splits)
        p = dm.splits[splitnum]
        range = p.first
        if splitdim_idx in range
            return splitnum
        end
    end
    throw(BoundsError("MatBlobs $(dm.sz) split on dimension $D", splitdim_idx))
end

function serialize(s::SerializationState, sm::MatBlobs)
    Serializer.serialize_type(s, typeof(sm))
    serialize(s, sm.metadir)
    serialize(s, sm.sz)
    serialize(s, sm.splits)

    coll = sm.coll
    serialize(s, coll.id)
    serialize(s, coll.mutability)
    serialize(s, coll.reader)
    serialize(s, coll.blobs)
    serialize(s, coll.maxcache)
end

function matblob(metadir::AbstractString; maxcache::Int=10)
    @logmsg("reading back matrix from $metadir")
    open(joinpath(metadir, "meta"), "r") do io
        mat = deserialize(SerializationState(io))
        mat.metadir = metadir
        max_cached!(mat.coll, maxcache)
        mat
    end
end

function flush(dm::MatBlobs, wrkrs::Vector{Int}=Int[]; callback::Bool=true)
    isempty(wrkrs) ? flush(dm.coll; callback=callback) : flush(dm.coll, wrkrs; callback=callback)
    nothing
end

##
# SparseMatBlobs specific functions
sersz(sp::SparseMatrixCSC{Tv,Ti}) where {Tv, Ti} = (sizeof(Int64)*3 + sizeof(sp.colptr) + sizeof(sp.rowval) + sizeof(sp.nzval))

function load(meta::FileMeta, reader::FileBlobIO{SparseMatrixCSC{Tv,Ti}}) where {Tv<:Real, Ti<:Integer}
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        header = Array{Int64}(3)
        pos1 = position(fhandle)
        header = read!(fhandle, header)
        m = header[1]
        n = header[2]
        nz = header[3] 

        pos1 += sizeof(header)
        colptr = reader.use_mmap ? blobmmap(fhandle, Vector{Ti}, (n+1,), pos1) : read!(fhandle, Array{Ti}(n+1))

        pos1 += sizeof(colptr)
        rowval = reader.use_mmap ? blobmmap(fhandle, Vector{Ti}, (nz,), pos1) : read!(fhandle, Array{Ti}(nz))

        pos1 += sizeof(rowval)
        nzval = reader.use_mmap ? blobmmap(fhandle, Vector{Tv}, (nz,), pos1) : read!(fhandle, Array{Tv}(nz))
        return SparseMatrixCSC{Tv,Ti}(m, n, colptr, rowval, nzval)
    end
end

function save(spm::SparseMatrixCSC{Tv,Ti}, meta::FileMeta, writer::FileBlobIO{SparseMatrixCSC{Tv,Ti}}) where {Tv<:Real, Ti<:Integer}
    if writer.use_mmap && ismmapped(spm.colptr) && ismmapped(spm.rowval) && ismmapped(spm.nzval)
        syncmmapped(spm.colptr)
        syncmmapped(spm.rowval)
        syncmmapped(spm.nzval)
    else
        header = Int64[spm.m, spm.n, length(spm.nzval)]

        touch(meta.filename)
        open(meta.filename, "r+") do fhandle
            seek(fhandle, meta.offset)
            write(fhandle, header)
            write(fhandle, spm.colptr)
            write(fhandle, spm.rowval)
            write(fhandle, spm.nzval)
        end
    end
    nothing
end

function load(sm::SparseMatBlobs, col::Int)
    splitnum = splitidx(sm, col)
    p = sm.splits[splitnum]
    range = p.first
    bid = p.second
    data = load(sm.coll, bid)
    #@logmsg("loaded split idx:$(col) from splitnum $splitnum ($bid) with range: $range, sz: $(size(data))")
    data, range
end

getindex(sm::SparseMatBlobs{Tv}, i::Int) where Tv = getindex(sm, ind2sub(size(sm), i)...)
function getindex(sm::SparseMatBlobs{Tv}, i1::Int, i2::Int) where Tv
    part, range = load(sm, i2)
    part[relidx(range, 2, i1, i2)...]
end

function getindex(sm::SparseMatBlobs{Tv}, ::Colon, i2::Int) where Tv
    part, range = load(sm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2]
end

function deserialize(s::SerializationState, ::Type{SparseMatBlobs{Tv,Ti}}) where {Tv, Ti}
    metadir = deserialize(s)
    sz = deserialize(s)
    splits = deserialize(s)

    coll_id = deserialize(s)
    coll_mut = deserialize(s)
    coll_reader = deserialize(s)
    coll_blobs = deserialize(s)
    coll_maxcache = deserialize(s)

    coll = BlobCollection(SparseMatrixCSC{Tv,Ti}, coll_mut, coll_reader; maxcache=coll_maxcache, id=coll_id)
    coll.blobs = coll_blobs
    SparseMatBlobs{Tv,Ti}(metadir, sz, splits, coll)
end

function SparseMatBlobs(::Type{Tv}, ::Type{Ti}, metadir::AbstractString, io::BlobIO=FileBlobIO(SparseMatrixCSC{Tv,Ti}, true); maxcache::Int=10) where {Tv, Ti}
    T = SparseMatrixCSC{Tv,Ti}
    mut = Mutable(BYTES_128MB, io)
    coll = BlobCollection(T, mut, io; maxcache=maxcache)
    SparseMatBlobs{Tv,Ti}(metadir, (0,0), Pair[], coll)
end

function append!(sp::SparseMatBlobs{Tv,Ti}, S::SparseMatrixCSC{Tv,Ti}) where {Tv, Ti}
    m,n = size(S)
    if isempty(sp.splits)
        sp.sz = (m, n)
        idxrange = 1:n
    else
        (sp.sz[1] == m) || throw(BoundsError("SparseMatBlobs $(sp.sz)", (m,n)))
        old_n = sp.sz[2]
        idxrange = (old_n+1):(old_n+n)
        sp.sz = (m, old_n+n)
    end

    fname = joinpath(sp.metadir, string(length(sp.splits)+1))
    meta = FileMeta(fname, 0, sersz(S))

    blob = append!(sp.coll, SparseMatrixCSC{Tv,Ti}, meta, locality(sp), Nullable(S))
    push!(sp.splits, idxrange => blob.id)
    @logmsg("appending blob $(blob.id) of size: $(size(S)) for idxrange: $idxrange, sersz: $(meta.size)")
    blob
end

function save(sp::SparseMatBlobs, wrkrs::Vector{Int}=Int[])
    isempty(wrkrs) ? save(sp.coll) : save(sp.coll, wrkrs)
    save(sp.coll)
    open(joinpath(sp.metadir, "meta"), "w") do io
        serialize(SerializationState(io), sp)
    end
    nothing
end

SparseMatBlobs(metadir::AbstractString; maxcache::Int=10) = matblob(metadir; maxcache=maxcache)

##
# DenseMatBlobs specific functions
sersz(d::Matrix{T}) where T = (sizeof(Int64)*2 + sizeof(d))

function load(dm::DenseMatBlobs{T,D,N}, splitdim_idx::Int) where {T, D, N}
    splitnum = splitidx(dm, splitdim_idx)
    p = dm.splits[splitnum]
    range = p.first
    bid = p.second
    load(dm.coll, bid), range
end

getindex(dm::DenseMatBlobs{T,D,N}, i::Int) where {T, D, N} = getindex(dm, ind2sub(size(dm), i)...)

function getindex(dm::DenseMatBlobs{T,D,N}, i1::Int, i2::Int) where {T, D, N}
    splitdim_idx = (D == 1) ? i1 : i2
    part, range = load(dm, splitdim_idx)
    part[relidx(range, D, i1, i2)...]
end

function getindex(dm::DenseMatBlobs{T,1,N}, i1::Int, ::Colon) where {T, N}
    part, range = load(dm, i1)
    reli1, reli2 = relidx(range, 1, i1, 1)
    part[reli1,:]
end

function getindex(dm::DenseMatBlobs{T,1,N}, idxs, ::Colon) where {T, N}
    res = Array{T}(length(idxs), N)
    for residx in 1:length(idxs)
        idx = idxs[residx]
        res[residx,:] = dm[idx,:]
    end
    res
end

function getindex(dm::DenseMatBlobs{T,2,N}, ::Colon, i2::Int) where {T, N}
    part, range = load(dm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2]
end

function getindex(dm::DenseMatBlobs{T,2,N}, ::Colon, idxs) where {T, N}
    res = Array{T}(N, length(idxs))
    for residx in 1:length(idxs)
        idx = idxs[residx]
        res[:, residx] = dm[:, idx]
    end
    res
end

setindex!(dm::DenseMatBlobs{T,D,N}, v::T, i::Int) where {T, D, N} = setindex!(dm, v, ind2sub(size(dm), i)...)

function setindex!(dm::DenseMatBlobs{T,D,N}, v::T, i1::Int, i2::Int) where {T, D, N}
    splitdim_idx = (D == 1) ? i1 : i2
    part, range = load(dm, splitdim_idx)
    part[relidx(range, D, i1, i2)...] = v
end

function setindex!(dm::DenseMatBlobs{T,1,N}, v, i1::Int, ::Colon) where {T, N}
    part, range = load(dm, i1)
    reli1, reli2 = relidx(range, 1, i1, 1)
    part[reli1,:] = v
end

function setindex!(dm::DenseMatBlobs{T,2,N}, v, ::Colon, i2::Int) where {T, N}
    part, range = load(dm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2] = v
end
#=
function *(A::Vector{T1}, B::DenseMatBlobs{T2,1}) where {T1, T2}
    T = promote_type(T1, T2)
    res = Array{T}(size(B, 2))
    for idx in 1:length(B.splits)
        p = B.splits[idx]
        part, r = load(B, first(p.first))
        res[r] = v * part
    end
    res
end
=#
function *(A::Matrix{T1}, B::DenseMatBlobs{T2,2}) where {T1, T2}
    m,n = size(B)
    (size(A, 2) == m) || throw(DimensionMismatch("A has dimensions $(size(A)) but B has dimensions $(size(B))"))
    res = Array{promote_type(T1,T2)}(1, n)
    for idx in 1:length(B.splits)
        p = B.splits[idx]
        part, r = load(B, first(p.first))
        res[r] = A * part
    end
    res
end

function deserialize(s::SerializationState, ::Type{DenseMatBlobs{T,D,N}}) where {T, D, N}
    metadir = deserialize(s)
    sz = deserialize(s)
    splits = deserialize(s)

    coll_id = deserialize(s)
    coll_mut = deserialize(s)
    coll_reader = deserialize(s)
    coll_blobs = deserialize(s)
    coll_maxcache = deserialize(s)

    coll = BlobCollection(Matrix{T}, coll_mut, coll_reader; maxcache=coll_maxcache, id=coll_id)
    coll.blobs = coll_blobs
    DenseMatBlobs{T,D,N}(metadir, sz, splits, coll)
end

function DenseMatBlobs(::Type{Tv}, D::Int, N::Int, metadir::AbstractString, io::BlobIO=FileBlobIO(Array{Tv}, true); maxcache::Int=10) where Tv
    T = Matrix{Tv}
    mut = Mutable(BYTES_128MB, io)
    coll = BlobCollection(T, mut, io; maxcache=maxcache)
    DenseMatBlobs{Tv,D,N}(metadir, (0,0), Pair[], coll)
end

function append!(dm::DenseMatBlobs{Tv,D,N}, M::Matrix{Tv}) where {Tv, D, N}
    m,n = size(M)
    unsplit_dim = (D == 1) ? n : m
    split_dim = (D == 1) ? m : n
    (N == unsplit_dim) || throw(BoundsError("DenseMatBlobs with unsplit dimension $D fixed at $N", (m,n)))
    if isempty(dm.splits)
        dm.sz = (m, n)
        idxrange = 1:split_dim
    else
        old_split_dim = dm.sz[D]
        new_split_dim = old_split_dim + split_dim
        idxrange = (old_split_dim+1):new_split_dim
        dm.sz = (D == 1) ? (new_split_dim, unsplit_dim) : (unsplit_dim, new_split_dim)
    end

    fname = joinpath(dm.metadir, string(length(dm.splits)+1))
    meta = FileMeta(fname, 0, sersz(M))

    blob = append!(dm.coll, Matrix{Tv}, meta, locality(dm), Nullable(M))
    push!(dm.splits, idxrange => blob.id)
    @logmsg("appending blob $(blob.id) of size: $(size(M)) for idxrange: $idxrange, sersz: $(meta.size)")
    blob
end

DenseMatBlobs(metadir::AbstractString; maxcache::Int=10) = matblob(metadir; maxcache=maxcache)

function save(dm::DenseMatBlobs, wrkrs::Vector{Int}=Int[])
    isempty(wrkrs) ? save(dm.coll) : save(dm.coll, wrkrs)
    open(joinpath(dm.metadir, "meta"), "w") do io
        serialize(SerializationState(io), dm)
    end
    nothing
end

end # module
