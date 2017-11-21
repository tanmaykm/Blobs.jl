using Blobs
if (Base.VERSION < v"0.7.0-")
using Base.Test
else
using Test
end

println("testing attributes...")
include("test_attributes.jl")

println("testing blobs...")
include("test_blobs.jl")
include("test_memlimit_blobs.jl")

#if !isless(Base.VERSION, v"0.5.0-")
#println("testing matrix blobs...")
#include("test_matrix_blobs.jl")
#end
