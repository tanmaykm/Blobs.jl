@everywhere using Blobs
@everywhere import Blobs: @logmsg, load
include(joinpath(dirname(@__FILE__), "../examples/matrix.jl"))
if (Base.VERSION < v"0.7.0-")
@everywhere using Base.Test
using MatrixBlobs
else
@everywhere using Test
using .MatrixBlobs
end
import .MatrixBlobs: load, splitidx

const FULLM = 10000
const FULLN = 2000
const DELTAM = 4000

function make_sparse_blobs(sz::Tuple{Int,Int}, sparsity::Float64, deltaN::Int, metadir::String)
    spblobs = SparseMatBlobs(Float64, Int64, metadir)
    startidx = 1
    @logmsg("startidx:$startidx, sz:$sz")
    M,N = sz
    while startidx <= N
        idxrange = startidx:min(N, startidx + deltaN)
        sp = sprand(M, length(idxrange), 0.01)
        @logmsg("idxrange: $idxrange, sz: $(size(sp))")
        append!(spblobs, sp)
        startidx = last(idxrange) + 1
    end

    @logmsg("saving sparsematarray")
    save(spblobs)
    spblobs
end

function test_sparse_mat_blobs()
    metadir = tempdir()
    sz = (FULLN, FULLM)
    spblobs = make_sparse_blobs(sz,  0.01, DELTAM, metadir)
    @test isfile(joinpath(metadir, "meta"))
    @test isfile(joinpath(metadir, "1"))

    println("loading sparse mat blobs from $metadir")
    smatblobs = SparseMatBlobs(metadir)
    for idx in 1:length(smatblobs.splits)
        p = smatblobs.splits[idx]
        r = p.first
        part, _r = load(smatblobs, first(r))
        @logmsg("got part of size: $(size(part)), with r: $r, _r:$_r")
        @test r == _r
        @test size(part) == (FULLN, length(r))
        println("verified part $idx")
    end

    @test size(smatblobs) == sz

    t1 = time()
    for idx in 1:FULLM
        if isless(Base.VERSION, v"0.5.0-")
            @test isa(smatblobs[:,idx], SparseMatrixCSC)
        else
            @test isa(smatblobs[:,idx], SparseVector)
        end
    end
    t2 = time()
    td = t2 - t1
    println("time for $FULLM column getindex: $td")
end

function make_dense_mat_blobs(sz::Tuple{Int,Int}, splitdim::Int, delta::Int, metadir::String)
    UD = (splitdim == 1) ? sz[2] : sz[1]
    SD = sz[splitdim]
    dmblobs = DenseMatBlobs(Int64, splitdim, UD, metadir)

    startidx = 1
    @logmsg("startidx:$startidx, sz:$sz")
    idx = 1
    while startidx <= SD
        idxrange = startidx:min(SD, startidx + delta)
        D = (splitdim == 1) ? (length(idxrange),UD) : (UD,length(idxrange))
        M = ones(Int64, D...) * idx
        @logmsg("idxrange: $idxrange, sz: $(size(M))")
        append!(dmblobs, M)
        startidx = last(idxrange) + 1
        idx += 1
    end

    @logmsg("saving densematarray")
    save(dmblobs)
    dmblobs
end

function test_dense_mat_blobs()
    metadir = tempdir()
    sz = (FULLM, FULLN)
    dmatblobs = make_dense_mat_blobs(sz, 1, DELTAM, metadir)
    @test isfile(joinpath(metadir, "meta"))
    @test isfile(joinpath(metadir, "1"))

    println("loading dense mat blobs from $metadir")
    dmatblobs = DenseMatBlobs(metadir)
    for idx in 1:length(dmatblobs.splits)
        p = dmatblobs.splits[idx]
        r = p.first
        part, _r = load(dmatblobs, first(r))
        @test r == _r
        @test size(part) == (length(r), FULLN)
        @test part == ones(Int, length(r), FULLN) * idx
        println("verified part $idx")
    end

    @test size(dmatblobs) == sz

    t1 = time()
    N = 10^6
    for idx in 1:N
        i1 = ceil(Int, rand() * (FULLM-1) + 1)
        i2 = ceil(Int, rand() * (FULLN-1) + 1)
        v = splitidx(dmatblobs, i1)
        #@logmsg("verifying that index ($i1, $i2) == $v")
        @test dmatblobs[i1, i2] == v
        #@logmsg("verified that index ($i1, $i2) == $v")
    end
    t2 = time()
    td = t2 - t1
    println("time for $N random getindex: $td at $(td/N) per getindex")

    t1 = time()
    N = 10^4
    for idx in 1:N
        dmatblobs[idx,:] = idx
    end
    t2 = time()
    td = t2 - t1
    println("time for $N row setindex: $td")

    t1 = time()
    N = 10^4
    for idx in 1:N
        @test maximum(dmatblobs[idx,:]) == idx
    end
    t2 = time()
    td = t2 - t1
    println("time for $N row getindex and check: $td")

    t1 = time()
    for idx in 1:length(dmatblobs.splits)
        p = dmatblobs.splits[idx]
        r = p.first
        println("setting $r,: to $idx")
        dmatblobs[r,:] = idx
    end
    t2 = time()
    td = t2 - t1
    println("time for non optimal setindex: $td")
end

test_sparse_mat_blobs()
test_dense_mat_blobs()
