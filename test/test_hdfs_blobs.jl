using Blobs
using Elly
include("../examples/hdfs_fileio.jl")
using HDFSBlobs
using Base.Test

import Blobs: @logmsg

const BSZ = 128*1024*1024
#const BSZ = 12*1024*1024
const NBLKS = 10

function test_hdfs_multiple_small_files(hdfsuri::String)
    T = Vector{Float64}
    io = HDFSBlobIO(T, hdfsuri)
    N = round(Int, BSZ / sizeof(Float64))
    mut = Mutable(N*sizeof(Float64), io)
    mem = ceil(Int, Base.Sys.free_memory()/3)
    coll = BlobCollection(T, mut, io; strategy=maxmem, maxcache=mem)

    v = rand(N)
    verify_sum = sum(v) * NBLKS
    t1 = time()
    for idx in 1:NBLKS
        @time blob = append!(coll, T, FileMeta(string(idx), 0, N*sizeof(Float64)), locality(io), Nullable(v))
        @logmsg("appended total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
    end
    @time save(coll, joinpath(HDFSFile(hdfsuri), "meta"), workers())
    t2 = time()
    @logmsg("average time to write a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    coll = BlobCollection(T, Immutable(), io; strategy=maxmem, maxcache=mem)
    load(coll, joinpath(HDFSFile(hdfsuri), "meta"))
    check_sum = 0.0
    idx = 0
    t1 = time()
    for b in blobids(coll)
        @time v = load(coll, b)
        @time check_sum += sum(v)
        idx += 1
        @logmsg("total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
    end
    t2 = time()
    @logmsg("average time to read a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    @test_approx_eq check_sum verify_sum
end

function test_hdfs_single_large_file(hdfsuri::String)
    T = Vector{Float64}
    io = HDFSBlobIO(T, hdfsuri)
    N = round(Int, BSZ / sizeof(Float64))
    mem = ceil(Int, Base.Sys.free_memory()/3)
    coll = BlobCollection(T, Immutable(), io; strategy=maxmem, maxcache=mem)

    v = rand(N)
    verify_sum = sum(v) * NBLKS
    hfile = joinpath(HDFSFile(hdfsuri), "data")
    t1 = time()
    open(hfile, "w") do hf
        for idx in 1:NBLKS
            @time write(hf, reinterpret(UInt8, v, (sizeof(v),)))
            start_bytes = (idx-1) * sizeof(v)
            blob = append!(coll, T, FileMeta("data", start_bytes, sizeof(v)), locality(io), Nullable(v))
            @logmsg("written total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
        end
    end
    save(coll, joinpath(HDFSFile(hdfsuri), "meta"), workers())
    t2 = time()
    @logmsg("average time to write a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    coll = BlobCollection(T, Immutable(), io; strategy=maxmem, maxcache=mem)
    load(coll, joinpath(HDFSFile(hdfsuri), "meta"))
    check_sum = 0.0
    idx = 0
    t1 = time()
    for b in blobids(coll)
        @time v = load(coll, b)
        @time check_sum += sum(v)
        idx += 1
        @logmsg("read total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
    end
    t2 = time()
    @logmsg("average time to read a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    @test_approx_eq check_sum verify_sum
end

function test_local_single_large_file(fileuri::String)
    T = Vector{Float64}
    N = round(Int, BSZ / sizeof(Float64))

    v = rand(N)
    verify_sum = sum(v) * NBLKS
    file = joinpath(fileuri, "data")
    t1 = time()
    open(file, "w") do f
        for idx in 1:NBLKS
            @time write(f, v)
            @logmsg("written total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
        end
    end
    t2 = time()
    @logmsg("average time to write a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    check_sum = 0.0
    idx = 0
    t1 = time()
    for idx in 1:NBLKS
        @time open(file) do f
            start_bytes = (idx-1) * sizeof(v)
            seek(f, start_bytes)
            read!(f, v)
        end
        @time check_sum += sum(v)
        @logmsg("read total $(idx * sizeof(v)) bytes, $(ceil(Int, idx * sizeof(v) / 1024 / 1024 / 1024)) GB")
    end
    t2 = time()
    @logmsg("average time to read a block: $((t2-t1)/NBLKS) sec @ $(NBLKS*sizeof(v)/(t2-t1)/1024/1024) MBps")

    @test_approx_eq check_sum verify_sum
end

function delete_test_files(hdfsuri::String)
    @logmsg("deleting all test files")
    for idx in 1:NBLKS
        hfile = joinpath(HDFSFile(hdfsuri), "$idx")
        exists(hfile) && rm(hfile)
    end
    hfile = joinpath(HDFSFile(hdfsuri), "meta")
    exists(hfile) && rm(hfile)
    hfile = joinpath(HDFSFile(hdfsuri), "data")
    exists(hfile) && rm(hfile)
    @logmsg("deleted all test files")
end

const hdfsuri = "hdfs://tan@localhost:9000/blobs/"
test_hdfs_multiple_small_files(hdfsuri)
delete_test_files(hdfsuri)
test_hdfs_single_large_file(hdfsuri)
