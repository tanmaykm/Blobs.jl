using Blobs
using Base.Test

println("testing attributes...")
include("test_attributes.jl")

println("testing blobs...")
include("test_blobs.jl")

println("testing densearray blobs...")
include("test_densearray_blobs.jl")
