@everywhere using DistributedBlobs
@everywhere using Base.Test
@everywhere import DistributedBlobs: @logmsg

function create_rand_file_backed_blob{T,L}(::Type{T}, ::Type{L}, sz::Int, nodeid::Int)
    meta = TypedMeta(FileBlobIO => FileMeta(tempname(), 0, sizeof(T)*sz), FunctionBlobIO => FunctionMeta((sizeof(T)*sz,)))
    Blob(T, L, meta, nodeid)
end

function create_blob_collection{T,L}(::Type{T}, ::Type{L}, sz::Int)
    # have a random number generator as reader and save to file
    mut = Mutable(sz*sizeof(T), FileBlobIO())
    reader = FunctionBlobIO((p)->rand(T, p))
    coll = BlobCollection(T, mut, reader)

    # create a blob for each worker
    for nodeid in workers()
        blob = create_rand_file_backed_blob(T, StrongLocality, sz, nodeid)
        append!(coll, blob)
    end
    bids = collect(blobids(coll))
    register(coll, workers())
    collid = coll.id
    @logmsg("created blob collection: $collid")

    # load all blobs
    @parallel for blobid in bids
        load(collid, blobid)
        println("initialized $collid -> $blobid")
    end
    @logmsg("initialized blobs")

    # save all blobs
    bcfile = tempname()
    save(coll, bcfile, workers())
    @logmsg("saved blobs. blob collection saved at $bcfile")
    bcfile, coll
end

function read_blob_collection{T,L}(::Type{T}, ::Type{L}, sz::Int, bcfile)
    coll = load(BlobCollection(T, Immutable(sz*sizeof(T)), FileBlobIO()), bcfile)
    register(coll, workers())

    collid = coll.id
    bids = collect(blobids(coll))
    @logmsg("read into blob collection: $collid")

    # load all blobs
    x = @parallel for blobid in bids
        d = load(collid, blobid)
        println("loaded $collid -> $blobid")
        @test length(d) == sz
    end
    println(fetch(x[1]))
    @logmsg("loaded blobs")
    coll
end

function cleanup_blob_collection(bcfile::AbstractString, coll::BlobCollection)
    @logmsg("cleaning up...")
    for blobid in blobids(coll)
        blob = coll.blobs[blobid]
        meta = blob.metadata.metadict[FileBlobIO]
        rm(meta.filename)
        @logmsg("cleaned up blob $(meta.filename)")
    end
    rm(bcfile)
    @logmsg("cleaned up blobs")
end

function test_blob{T,L}(::Type{T}, ::Type{L}, sz::Int)
    bcfile, coll = create_blob_collection(T, L, sz)         # create blob collection
    deregister(coll, procs())                               # deregister blob collection
    coll = read_blob_collection(T, L, sz, bcfile)           # restore the saved blob collection
    cleanup_blob_collection(bcfile, coll)                   # clean up
end

test_blob(Int64, StrongLocality, 10240)
