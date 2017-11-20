@everywhere using Blobs
@everywhere import Blobs: @logmsg
if (Base.VERSION < v"0.7.0-")
@everywhere using Base.Test
else
@everywhere using Test
end

function create_rand_file_backed_blob(::Type{T}, ::Type{L}, sz::Int, nodeid::Int) where {T, L}
    VT = Vector{T}
    meta = TypedMeta(FileBlobIO{VT} => FileMeta(tempname(), 0, sizeof(T)*sz), FunctionBlobIO{VT} => FunctionMeta((sizeof(T)*sz,)))
    Blob(VT, L, meta, nodeid)
end

function create_blob_collection(::Type{T}, ::Type{L}, sz::Int) where {T, L}
    VT = Vector{T}
    # have a random number generator as reader and save to file
    mut = Mutable(sz*sizeof(T), FileBlobIO(VT))
    reader = FunctionBlobIO(VT, (p)->rand(T, floor(Int,p/sizeof(T))))
    coll = BlobCollection(VT, mut, reader)

    # create a blob for each worker
    for nodeid in workers()
        blob = create_rand_file_backed_blob(T, StrongLocality, sz, nodeid)
        append!(coll, blob)
    end
    bids = collect(blobids(coll))
    register(coll, workers())
    collid = coll.id
    @logmsg("created blob collection: $collid, blob ids: $bids")

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

function read_blob_collection(::Type{T}, ::Type{L}, sz::Int, bcfile) where {T, L}
    VT = Vector{T}
    coll = load(BlobCollection(VT, Immutable(sz*sizeof(T)), FileBlobIO(VT)), bcfile)
    register(coll, workers())

    collid = coll.id
    bids = collect(blobids(coll))
    @logmsg("read into blob collection: $collid")

    # load all blobs
    @parallel for blobid in bids
        d = load(collid, blobid)
        println("loaded $collid -> $blobid")
        @test length(d) == sz
    end
    @logmsg("loaded blobs")
    coll
end

function cleanup_blob_collection(::Type{T}, bcfile::String, coll::BlobCollection) where T
    VT = Vector{T}
    @logmsg("cleaning up...")
    for blobid in blobids(coll)
        blob = coll.blobs[blobid]
        meta = blob.metadata.metadict[FileBlobIO{VT}]
        if isfile(meta.filename)
            rm(meta.filename)
            @logmsg("cleaned up blob $(meta.filename)")
        else
            @logmsg("not cleaning up blob $(meta.filename) (was not found)")
        end
    end
    rm(bcfile)
    @logmsg("cleaned up blobs")
end

function test_blob(::Type{T}, ::Type{L}, sz::Int) where {T, L}
    bcfile, coll = create_blob_collection(T, L, sz)         # create blob collection
    deregister(coll, procs())                               # deregister blob collection
    coll = read_blob_collection(T, L, sz, bcfile)           # restore the saved blob collection
    cleanup_blob_collection(T, bcfile, coll)                # clean up
end

test_blob(Int64, StrongLocality, 10240)
