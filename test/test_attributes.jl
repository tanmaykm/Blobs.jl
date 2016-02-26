using Blobs
using Base.Test

const iplist1 = [ip"54.204.24.10", ip"54.204.24.11"]
const iplist2 = [ip"54.204.24.20", ip"54.204.24.21"]
const hostlist1 = ["host1"]
const hostlist2 = ["host2"]

function test_default_nodemap()
    nodes, ips, hns = localto(myid())
    @test myid() in nodes
    @test getipaddr() in ips
    @test gethostname() in hns
end

function create_test_nodemap()
    nodemap = NodeMap()

    for nid in 2:3
        node = Node(nid, iplist1[(nid-1):(nid-1)], hostlist1)
        addnode(nodemap, node)
    end
    for nid in 4:7
        node = Node(nid, iplist2, hostlist2)
        addnode(nodemap, node)
    end
    nodemap
end

function test_nodemap()
    nodemap = create_test_nodemap()

    for nid in 2:3
        nodes, ips, hns = localto(nid, nodemap)
        @test length(nodes) == 2
        @test length(ips) == 2
        @test length(hns) == 1
        for ip in iplist1
            @test ip in ips
        end
        for hn in hostlist1
            @test hn in hns
        end
    end

    for nid in 4:7
        nodes, ips, hns = localto(nid, nodemap)
        @test length(nodes) == 4
        @test length(ips) == 2
        @test length(hns) == 1
        for ip in iplist2
            @test ip in ips
        end
        for hn in hostlist2
            @test hn in hns
        end
    end
end

function test_locality()
    nodemap = create_test_nodemap()
    loc1 = StrongLocality(2, 3, iplist1..., hostlist1...)
    loc2 = WeakLocality(4, 5, 6, 7, iplist2..., hostlist2...)

    @test !islocal(loc1, 4)
    @test islocal(loc1, 2)
    @test islocal(loc2, 4)
    @test !islocal(loc2, 2)
    @test !islocal(loc2, 1)
end

test_default_nodemap()
test_nodemap()
test_locality()
