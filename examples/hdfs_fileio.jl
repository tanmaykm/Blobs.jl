module HDFSBlobs

using Blobs
using Elly
import Base: open
import Blobs: @logmsg, open, load, save, locality

export open, locality, load, save, HDFSBlobIO

immutable HDFSBlobIO{T} <: BlobIO{T}
    uri::AbstractString    # the HDFS URI prefix with server credentials and location (e.g.: "hdfs://userid@localhost:9000")
end
HDFSBlobIO{T}(::Type{T}, uri::AbstractString) = HDFSBlobIO{T}(strip(uri))

function open(io::HDFSBlobIO, meta::FileMeta, mode::AbstractString="r")
    hfile = joinpath(HDFSFile(io.uri), meta.filename)
    if mode == "w"
        dn = joinpath(HDFSFile(io.uri), dirname(meta.filename))
        (exists(dn) && isdir(dn)) || mkpath(dn)
        exists(hfile) || touch(hfile)
    end
    open(hfile, mode)
end

# file blob io
# hdfs files have weak locality to the machines that have the block(s)
function locality{T}(::Type{HDFSBlobIO{T}}, nodemap::NodeMap=DEF_NODE_MAP)
    nodes, ips, hns = localto(myid(), nodemap)
    WeakLocality(nodes..., ips..., hns...)
end

function load{T<:Real}(meta::FileMeta, reader::HDFSBlobIO{Vector{T}})
    @logmsg("load using HDFSBlobIO{Vector{$T}}")
    sz = floor(Int, meta.size / sizeof(T))
    open(reader, meta) do f
        (meta.offset > 0) && seek(f, meta.offset)
        databytes = Array(T, sz)
        read!(f, reinterpret(UInt8, databytes, (sizeof(databytes),)))
        return databytes
    end
end

function save{T<:Real}(databytes::Vector{T}, meta::FileMeta, writer::HDFSBlobIO{Vector{T}})
    @logmsg("save Vector{$T} using HDFSBlobIO{Vector{$T}}")
    open(writer, meta, "w") do f
        #@logmsg("writing $(typeof(databytes)) array of length $(length(databytes)), size $(sizeof(databytes)). expected: $(meta.size)")
        (sizeof(databytes) == meta.size) || throw(ArgumentError("Blob data not of expected size. Got $(sizeof(databytes)), expected $(meta.size)."))
        write(f, reinterpret(UInt8, databytes, (sizeof(databytes),)))
    end
    nothing
end

function load{T<:Real}(meta::FileMeta, reader::HDFSBlobIO{Array{T}})
    @logmsg("load using HDFSBlobIO{Array{$T}}")
    open(reader, meta) do fhandle
        (meta.offset > 0) && seek(fhandle, meta.offset)
        hdrsz = read(fhandle, Int64)
        header = Array(Int64, hdrsz)
        read!(fhandle, reinterpret(UInt8, header, (sizeof(header),)))
        data = Array(T, header...)
        read!(fhandle, reinterpret(UInt8, data, (sizeof(data),)))
        return data
    end
end

function save{T<:Real}(M::Array{T}, meta::FileMeta, writer::HDFSBlobIO{Array{T}})
    @logmsg("save Array{$T} using HDFSBlobIO{Array{$T}}")
    header = Int64[size(M)...]
    hdrsz = Int64(length(header))

    open(writer, meta, "w") do fhandle
        write(fhandle, hdrsz)
        write(fhandle, reinterpret(UInt8, header, (sizeof(header),)))
        write(fhandle, reinterpret(UInt8, M, (sizeof(M),)))
    end
    nothing
end

function load(meta::FileMeta, reader::HDFSBlobIO{Any})
    @logmsg("load using HDFSBlobIO{Any}")
    open(reader, meta) do fhandle
        (meta.offset > 0) && seek(fhandle, meta.offset)
        return deserialize(SerializationState(fhandle))
    end
end

function save(data, meta::FileMeta, writer::HDFSBlobIO{Any})
    @logmsg("save $(typeof(data)) using HDFSBlobIO{Any}")
    open(writer, meta, "w") do fhandle
        serialize(SerializationState(fhandle), data)
    end
end

function save(coll::BlobCollection, hfile::HDFSFile, wrkrs::Vector{Int})
    dn = dirname(hfile)
    (exists(dn) && isdir(dn)) || mkpath(dn)
    open(hfile, "w") do f
        save(coll, f, wrkrs)
    end
end

function load(coll::BlobCollection, hfile::HDFSFile)
    open(hfile) do f
        load(coll, f)
    end
end

end # module
