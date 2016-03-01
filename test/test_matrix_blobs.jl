@everywhere using Blobs
@everywhere using Base.Test
@everywhere import Blobs: @logmsg, load
include(joinpath(dirname(@__FILE__), "../examples/matrix.jl"))
using MatrixBlobs
using MatrixBlobs: load, splitidx

const FULLM = 10000
const FULLN = 2000
const DELTAM = 4000

function test_sparse_mat_blobs()
    metadir = tempdir()
    sz = (FULLN, FULLM)
    dmatblobs = SparseMatBlobs(SparseMatrixCSC{Float64,Int64}, sz,  0.01, DELTAM, metadir)
    @test isfile(joinpath(metadir, "meta"))
    @test isfile(joinpath(metadir, "1"))

    println("loading sparse mat blobs from $metadir")
    smatblobs = SparseMatBlobs(metadir)
    for idx in 1:length(smatblobs.splits)
        p = smatblobs.splits[idx]
        r = p.first
        part, _r = load(smatblobs, first(r))
        @test r == _r
        @test size(part) == (FULLN, length(r))
        println("verified part $idx")
    end

    @test size(smatblobs) == sz

    t1 = time()
    for idx in 1:FULLM
        @test isa(smatblobs[:,idx], SparseVector)
    end
    t2 = time()
    td = t2 - t1
    println("time for $FULLM column getindex: $td")
end

function test_dense_mat_blobs()
    metadir = tempdir()
    sz = (FULLM, FULLN)
    dmatblobs = DenseMatBlobs(Int64, 1, sz, metadir)
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

test_dense_mat_blobs()
test_sparse_mat_blobs()
