module MatrixBlobs

using Blobs
import Blobs: @logmsg, load, save
using Base.Random: UUID
import Base: serialize, deserialize, getindex, setindex!, size, append!

export DenseMatBlobs, SparseMatBlobs, size, getindex, setindex!, serialize, deserialize, save, load

const BYTES_128MB = 128 * 1024 * 1024

def_sz{T}(::Type{T}, blk_sz::Int=BYTES_128MB) = floor(Int, blk_sz / sizeof(T))

function relidx(range::Range, D::Int, i1::Int, i2::Int)
    if D == 1
        (i1 - first(range) + 1), i2
    else
        i1, (i2 - first(range) + 1)
    end
end

type SparseMatBlobs{Tv,Ti} <: AbstractMatrix{Tv}
    metadir::AbstractString
    sz::Tuple
    splits::Vector{Pair}
    coll::BlobCollection{SparseMatrixCSC{Tv,Ti}}
end

# D = dimension that is split
# N = value of the other dimension, which is constant across all splits
type DenseMatBlobs{T,D,N} <: AbstractMatrix{T}
    metadir::AbstractString
    sz::Tuple
    splits::Vector{Pair}        # keep a mapping of index ranges to blobs
    coll::BlobCollection{Matrix{T}}
end

typealias MatBlobs Union{SparseMatBlobs,DenseMatBlobs}


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
    serialize(s, sm.sz)
    serialize(s, sm.splits)

    coll = sm.coll
    serialize(s, coll.id)
    serialize(s, coll.mutability)
    serialize(s, coll.reader)
    serialize(s, coll.blobs)
    serialize(s, coll.maxcache)
end

function matblob(metadir::AbstractString)
    @logmsg("reading back matrix from $metadir")
    open(joinpath(metadir, "meta"), "r") do io
        deserialize(SerializationState(io))
    end
end

##
# SparseMatBlobs specific functions
sersz{Tv,Ti}(sp::SparseMatrixCSC{Tv,Ti}) = (sizeof(Int64)*3 + sizeof(sp.colptr) + sizeof(sp.rowval) + sizeof(sp.nzval))

function load{Tv<:Real,Ti<:Integer}(meta::FileMeta, reader::FileBlobIO{SparseMatrixCSC{Tv,Ti}})
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        header = Array(Int64, 3)
        pos1 = position(fhandle)
        header = read!(fhandle, header)
        m = header[1]
        n = header[2]
        nz = header[3] 

        pos1 += sizeof(header)
        colptr = reader.use_mmap ? Mmap.mmap(fhandle, Vector{Ti}, (n+1,), pos1) : read!(fhandle, Array(Ti, n+1))

        pos1 += sizeof(colptr)
        rowval = reader.use_mmap ? Mmap.mmap(fhandle, Vector{Ti}, (nz,), pos1) : read!(fhandle, Array(Ti, nz))

        pos1 += sizeof(rowval)
        nzval = reader.use_mmap ? Mmap.mmap(fhandle, Vector{Tv}, (nz,), pos1) : read!(fhandle, Array(Tv, nz))
        return SparseMatrixCSC{Tv,Ti}(m, n, colptr, rowval, nzval)
    end
end

function save{Tv<:Real,Ti<:Integer}(spm::SparseMatrixCSC{Tv,Ti}, meta::FileMeta, writer::FileBlobIO{SparseMatrixCSC{Tv,Ti}})
    header = Int64[spm.m, spm.n, length(spm.nzval)]

    touch(meta.filename)
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        write(fhandle, header)
        write(fhandle, spm.colptr)
        write(fhandle, spm.rowval)
        write(fhandle, spm.nzval)
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

getindex{Tv}(sm::SparseMatBlobs{Tv}, i::Int) = getindex(sm, ind2sub(size(sm), i)...)
function getindex{Tv}(sm::SparseMatBlobs{Tv}, i1::Int, i2::Int)
    part, range = load(sm, i2)
    part[relidx(range, 2, i1, i2)...]
end

function getindex{Tv}(sm::SparseMatBlobs{Tv}, ::Colon, i2::Int)
    part, range = load(sm, i2)
    reli1, reli2 = relidx(range, 2, 1, i2)
    part[:,reli2]
end

function deserialize{Tv,Ti}(s::SerializationState, ::Type{SparseMatBlobs{Tv,Ti}})
    sz = deserialize(s)
    splits = deserialize(s)

    coll_id = deserialize(s)
    coll_mut = deserialize(s)
    coll_reader = deserialize(s)
    coll_blobs = deserialize(s)
    coll_maxcache = deserialize(s)

    coll = BlobCollection(SparseMatrixCSC{Tv,Ti}, coll_mut, coll_reader; maxcache=coll_maxcache, id=coll_id)
    coll.blobs = coll_blobs
    SparseMatBlobs{Tv,Ti}("", sz, splits, coll)
end

function SparseMatBlobs{Tv,Ti}(::Type{Tv}, ::Type{Ti}, metadir::AbstractString)
    T = SparseMatrixCSC{Tv,Ti}
    mut = Mutable(BYTES_128MB, FileBlobIO(T, true))
    coll = BlobCollection(T, mut, FileBlobIO(T, true))
    SparseMatBlobs{Tv,Ti}(metadir, (0,0), Pair[], coll)
end

function append!{Tv,Ti}(sp::SparseMatBlobs{Tv,Ti}, S::SparseMatrixCSC{Tv,Ti})
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

    blob = append!(sp.coll, SparseMatrixCSC{Tv,Ti}, meta, StrongLocality(myid()), Nullable(S))
    push!(sp.splits, idxrange => blob.id)
    @logmsg("appending blob $(blob.id) of size: $(size(S)) for idxrange: $idxrange, sersz: $(meta.size)")
    blob
end

function save(sp::SparseMatBlobs)
    save(sp.coll)
    open(joinpath(sp.metadir, "meta"), "w") do io
        serialize(SerializationState(io), sp)
    end
    nothing
end

SparseMatBlobs(metadir::AbstractString) = matblob(metadir)

##
# DenseMatBlobs specific functions
sersz{T}(d::Matrix{T}) = (sizeof(Int64)*2 + sizeof(d))

function load{T<:Real}(meta::FileMeta, reader::FileBlobIO{Matrix{T}})
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        header = Array(Int64, 2)
        pos1 = position(fhandle)
        header = read!(fhandle, header)
        m = header[1]
        n = header[2]

        pos1 += sizeof(header)
        data = reader.use_mmap ? Mmap.mmap(fhandle, Matrix{T}, (m,n), pos1) : read!(fhandle, Array(T, m, n))
        return data
    end
end

function save{T<:Real}(M::Matrix{T}, meta::FileMeta, writer::FileBlobIO{Matrix{T}})
    header = Int64[size(M)...]

    touch(meta.filename)
    open(meta.filename, "r+") do fhandle
        seek(fhandle, meta.offset)
        write(fhandle, header)
        write(fhandle, M)
    end
    nothing
end

function load{T,D,N}(dm::DenseMatBlobs{T,D,N}, splitdim_idx::Int)
    splitnum = splitidx(dm, splitdim_idx)
    p = dm.splits[splitnum]
    range = p.first
    bid = p.second
    load(dm.coll, bid), range
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

function deserialize{T,D,N}(s::SerializationState, ::Type{DenseMatBlobs{T,D,N}})
    sz = deserialize(s)
    splits = deserialize(s)

    coll_id = deserialize(s)
    coll_mut = deserialize(s)
    coll_reader = deserialize(s)
    coll_blobs = deserialize(s)
    coll_maxcache = deserialize(s)

    coll = BlobCollection(Matrix{T}, coll_mut, coll_reader; maxcache=coll_maxcache, id=coll_id)
    coll.blobs = coll_blobs
    DenseMatBlobs{T,D,N}("", sz, splits, coll)
end

function DenseMatBlobs{Tv}(::Type{Tv}, D::Int, N::Int, metadir::AbstractString)
    T = Matrix{Tv}
    mut = Mutable(BYTES_128MB, FileBlobIO(T, true))
    coll = BlobCollection(T, mut, FileBlobIO(T, true))
    DenseMatBlobs{Tv,D,N}(metadir, (0,0), Pair[], coll)
end

function append!{Tv,D,N}(dm::DenseMatBlobs{Tv,D,N}, M::Matrix{Tv})
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

    blob = append!(dm.coll, Matrix{Tv}, meta, StrongLocality(myid()), Nullable(M))
    push!(dm.splits, idxrange => blob.id)
    @logmsg("appending blob $(blob.id) of size: $(size(M)) for idxrange: $idxrange, sersz: $(meta.size)")
    blob
end

DenseMatBlobs(metadir::AbstractString) = matblob(metadir)

function save(dm::DenseMatBlobs)
    save(dm.coll)
    open(joinpath(dm.metadir, "meta"), "w") do io
        serialize(SerializationState(io), dm)
    end
    nothing
end

end # module
