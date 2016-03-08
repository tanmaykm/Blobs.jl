__precompile__(true)

module Blobs

using Compat

using Base.Random: UUID, uuid4
using Base.Mmap: sync!
using Base: IPAddr
import Base: serialize, deserialize, append!, flush

export ismmapped, delmmapped, syncmmapped, blobmmap
export Locality, StrongLocality, WeakLocality
export Mutability, Mutable, Immutable
export Node, NodeMap, nodeids, addnode, localto, islocal
export BlobMeta, TypedMeta, FileMeta, FunctionMeta
export BlobIO, NoopBlobIO, FileBlobIO, FunctionBlobIO
export Blob, BlobCollection, blobids, load, save, serialize, deserialize, register, deregister, append!, flush, max_cached, max_cached!
export ProcessGlobalBlob
export maxmem, maxcount

# enable logging only during debugging
#using Logging
#const logger = Logging.configure(level=DEBUG)
##const logger = Logging.configure(filename="/tmp/blobs$(getpid()).log", level=DEBUG)
#macro logmsg(s)
#    quote
#        debug($(esc(s)))
#    end
#end
macro logmsg(s)
end
#macro logmsg(s)
#    quote
#        info($(esc(s)))
#    end
#end


include("cache/blobcache.jl")
using .BlobCache

include("attributes.jl")
include("blob.jl")
include("procglobal.jl")

function __init__()
    global const mmapped = Set{UInt}()
    global const BLOB_REGISTRY = Dict{UUID,BlobCollection}()
    global const DEF_NODE_MAP = initnodemap()
end

end # module
