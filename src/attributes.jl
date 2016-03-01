##
# Blob IO
# Handles reading and writing from sources.
# Writers are used only is blob is mutable.
abstract BlobIO{T}
type NoopBlobIO{T} <: BlobIO{T}
end
NoopBlobIO() = NoopBlobIO{Any}()

immutable FileBlobIO{T} <: BlobIO{T}
    use_mmap::Bool
end
FileBlobIO{T}(::Type{T}, use_mmap::Bool=false) = FileBlobIO{T}(use_mmap)

def_fn_writer(b,p...) = throw(InvalidStateException("IO configured only as", :reader))
immutable FunctionBlobIO{T} <: BlobIO{T}
    reader::Function
    writer::Function
end
FunctionBlobIO{T}(::Type{T}, reader::Function, writer::Function=def_fn_writer) = FunctionBlobIO{T}(reader, writer)

locality(io, nodemap=DEF_NODE_MAP) = locality(typeof(io), nodemap)

##
# Mutability
# Blobs can be mutable. They can be written or appended to. Since blobs have a maximum size limit, writing beyond the size fails with a spill.
# The spill callback is called at that point, which either throws an exception, or allocates a new blob. Spill callbacks are registered by
# blob groups.

defmutablespillcb(a...) = error("blob max size reached")
defimmutablespillcb(a...) = error("blob is immutable")

immutable Mutability{N}
    maxsize::Int
    writer::BlobIO
    spillcb::Function
    function Mutability(maxsize::Int=0, writer::BlobIO=NoopBlobIO(), spillcb::Function=(N==0)?defimmutablespillcb:defmutablespillcb)
        new(maxsize, writer, spillcb)
    end
end

typealias Immutable    Mutability{false}
typealias Mutable      Mutability{true}

##
# Blobs can be located at one or more processes, or physical nodes.
# Locality is determined by the blob reader/writer implementation.
# Locality can be:
# - Strong
#     - can only be accessed on the actual pid (remote ref), or IP/hostname (local files)
#     - may not be failsafe as they may go away if the compute node holding it fails
#     - accessing these blobs from non local processes entail a call to a remote Julia process
# - Weak
#     - can be accessed from anywhere
#     - may be preferably on some nodes
#     - generally backed by some kind of distributed/shared storage and are not affected by compute node failures
type Locality{N}
    nodes::Set{Int}
    ips::Set{IPAddr}
    hostnames::Set{AbstractString}

    function Locality(locs...)
        nodes = Set{Int}()
        ips = Set{IPAddr}()
        hostnames = Set{AbstractString}()
        isempty(locs) && (locs = localto(myid()))
        for loc in locs
            isa(loc, Integer) ? push!(nodes, loc) :
            isa(loc, IPAddr) ? push!(ips, loc) : push!(hostnames, string(loc)) 
        end
        new(nodes, ips, hostnames)
    end
end

typealias WeakLocality      Locality{:weak}
typealias StrongLocality    Locality{:strong}

function islocal_broad(loc::Locality, attr)
    nodes, ips, hns = localto(attr)
    !isempty(intersect(nodes, loc.nodes)) || !isempty(intersect(ips, loc.ips)) || !isempty(intersect(hns, loc.hostnames))
end
islocal(loc::Locality, nodeid::Int) = (nodeid in loc.nodes) || islocal_broad(loc, nodeid)
islocal(loc::Locality, ip::IPAddr) = (ip in loc.ips) || islocal_broad(loc, ip)
islocal(loc::Locality, hn::AbstractString) = (hn in loc.hostnames) || islocal_broad(loc, hn)


##
# Nodes and Node Maps
# A node represents a participating Julia process.
# The node Id is typically the pid, when using a Julia cluster manager.
# It can be different if the Julia processes are independently brought up by a different cluster manager.
#
# A node map holds a mapping of the node to the IP address / hostname of the physical machine it is currently running on.
# This is used to map affinities to nodes and facilitate data movement.
 
type Node
    nodeid::Int
    ips::Vector{IPAddr}
    hostnames::Vector{AbstractString}
end

type NodeMap
    props::Dict{Int,Node}
    ipgroup::Dict{IPAddr,Vector{Int}}
    hostnamegroup::Dict{AbstractString,Vector{Int}}

    function NodeMap()
        new(Dict{Int,Node}(), Dict{IPAddr,Vector{Int}}(), Dict{AbstractString,Vector{Int}}())
    end
end

nodeids(nodemap::NodeMap) = keys(nodemap.props)

function addnode(nodemap::NodeMap, node::Node)
    nodeid = node.nodeid
    nodemap.props[nodeid] = node
    for ip in node.ips
        grp = get!(nodemap.ipgroup, ip, Int[])
        push!(grp, nodeid)
    end
    for hn in node.hostnames
        grp = get!(nodemap.hostnamegroup, hn, Int[])
        push!(grp, nodeid)
    end
end

function initnodemap(nodemap::NodeMap=NodeMap())
    # add self info
    node = Node(myid(), [getipaddr()], [gethostname()])
    addnode(nodemap, node)

    # add workers
    for nid in workers()
        node = Node(nid, [remotecall_fetch(getipaddr, nid)], [remotecall_fetch(gethostname, nid)])
        addnode(nodemap, node)
    end
    nodemap
end

const DEF_NODE_MAP = initnodemap()

# localto methods can be used to get a list of nodes, ips, hostnames that are local to the given entity as per the nodemap
localto(ip::IPAddr, nodemap::NodeMap=DEF_NODE_MAP) = localto(get(nodemap.ipgroup, ip, Int[]), nodemap)
localto(hostname::AbstractString, nodemap::NodeMap=DEF_NODE_MAP) = localto(get(nodemap.hostnamegroup, hostname, Int[]), nodemap)
function localto(nodeids::Vector{Int}, nodemap::NodeMap=DEF_NODE_MAP)
    isempty(nodeids) ? (Int[], IPAddr[], AbstractString[]) : localto(nodeids[1], nodemap)
end
function _addnode(nodemap::NodeMap, nodes::Set{Int}, ips::Set{IPAddr}, hns::Set{AbstractString}, nodeid::Int)
    attr = nodemap.props[nodeid]
    push!(nodes, nodeid)
    for ip in attr.ips
        push!(ips, ip)
    end
    for hn in attr.hostnames
        push!(hns, hn)
    end
end
function localto(nodeid::Int, nodemap::NodeMap=DEF_NODE_MAP)
    nodes = Set{Int}(nodeid)
    ips = Set{IPAddr}()
    hns = Set{AbstractString}()

    if nodeid in keys(nodemap.props)
        myattr = nodemap.props[nodeid]
        # for each ip and hostname of nodeid, get other nodes and add their location
        for ip in myattr.ips
            union!(nodes, get(nodemap.ipgroup, ip, Int[]))
        end
        for hn in myattr.hostnames
            union!(nodes, get(nodemap.hostnamegroup, hn, Int[]))
        end
        for node in nodes
            _addnode(nodemap, nodes, ips, hns, node)
        end
    end
    nodes, ips, hns
end
