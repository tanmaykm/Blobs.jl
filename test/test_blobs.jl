@everywhere using DistributedBlobs
@everywhere using Base.Test
@everywhere import DistributedBlobs: @logmsg

function create_temp_file_blob{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int, nodeid::Int)
    locality = L(localto(nodeid, nodemap))
    fmeta = FileMeta(tempname(), 0, sizeof(T)*sz)
    Blob(T, fmeta, locality)
end

function create_rand_blob{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int, nodeid::Int)
    locality = L((nodeid))
    fmeta = FunctionMeta(T, sz)
    Blob(T, fmeta, locality)
end

function create_rand_file_blob{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int, nodeid::Int)
    locality = L(nodeid)
    typedmeta = TypedMeta()
    typedmeta.metadict[FileBlobIO] = FileMeta(tempname(), 0, sizeof(T)*sz)
    typedmeta.metadict[FunctionBlobIO] = FunctionMeta(T, sizeof(T)*sz)
    Blob(T, typedmeta, locality)
end

function create_blob_collection{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int)
    mutability = Mutable(sz*sizeof(T), FileBlobIO())
    reader = FunctionBlobIO((b,p)->rand!(reinterpret(p,b)), (b,p)->error("only reader"))
    coll = BlobCollection(Int64, mutability, reader, 10, nodemap)
    @logmsg("created blob collection: $(coll.id)")

    # have a random number generator as reader and save to file
    for nodeid in nodeids(nodemap)
        blob = create_rand_file_blob(nodemap, Int64, StrongLocality, sz, nodeid)
        @logmsg("appending blob $blob")
        append!(coll, blob)
    end

    # load all blobs
    for blobid in blobids(coll)
        load(coll, blobid)
    end
    @logmsg("loaded all blobs")

    # save all blobs
    bcfile = tempname()
    open(bcfile, "w") do fbcfile
        save(coll, fbcfile)
    end
    @logmsg("saved all blobs. blob collection saved at $bcfile")
    deregister(coll)
    @logmsg("deregistered collection")
    bcfile
end

function read_blob_collection{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int, bcfile)
    coll = BlobCollection(Int64, Immutable(sz*sizeof(T)), FileBlobIO(), 10, nodemap)
    @logmsg("reading into blob collection: $(coll.id)")
    open(bcfile) do fbcfile
        load(coll, fbcfile)
    end
    @logmsg("read into blob collection: $(coll.id)")
    for w in workers()
        remotecall_wait(register, w, coll)
    end
    @logmsg("registered blob everywhere")

    # load all blobs
    for blobid in blobids(coll)
        d = load(coll, blobid)
        @test length(d) == sz
    end
    @logmsg("loaded all blobs")
    coll
end

function cleanup_blob_collection(coll::BlobCollection)
    @logmsg("cleaning up...")
    for blobid in blobids(coll)
        blob = coll.blobs[blobid]
        meta = blob.metadata.metadict[FileBlobIO]
        rm(meta.filename)
        @logmsg("cleaned up blob $(meta.filename)")
    end
    @logmsg("cleaned up all blobs")
end

function test_blob{T,L}(nodemap::NodeMap, ::Type{T}, ::Type{L}, sz::Int)
    bcfile = create_blob_collection(nodemap, T, L, sz)
    # load the blobs
    coll = read_blob_collection(nodemap, T, L, sz, bcfile)
    cleanup_blob_collection(coll)
    rm(bcfile)
    @logmsg("cleaned up blob collection")
end

test_blob(DistributedBlobs.DEF_NODE_MAP, Int64, StrongLocality, 10240)
