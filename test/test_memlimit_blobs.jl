using Blobs
using Base.Test
import Blobs: @logmsg

const SZ_100MB = 100*1024*1024
function test_memlimit()
    PB = ProcessGlobalBlob(SZ_100MB, maxmem)
    for idx in 1:12
        A = Array(UInt8, 10*1024*1024)
        append!(PB, A)
        @test length(PB.coll.cache) <= 10
        @test length(PB.coll.blobs) == idx
    end
end

test_memlimit()
