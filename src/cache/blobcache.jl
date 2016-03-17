module BlobCache

export LRU, @get!, maxcount, maxmem

include("list.jl")

# Default cache size
const __MAXCACHE__ = 100
noop(k,v) = nothing

type LRU{K,V} <: Associative{K,V}
    ht::Dict{K, LRUNode{K, V}}
    q::LRUList{K, V}
    maxsize::Int
    cb::Function
    isfull::Function

    LRU(m::Int=__MAXCACHE__; callback::Function=noop, strategy::Function=maxcount) = new(Dict{K, V}(), LRUList{K, V}(), m, callback, strategy)
end
LRU(m::Int=__MAXCACHE__) = LRU{Any, Any}(m)

function maxcount(lru::LRU)
    length(lru) > lru.maxsize
end

function maxmem(lru::LRU)
    memused = 0
    for v in values(lru.ht)
        memused += Base.summarysize(v.v)
    end
    memused > lru.maxsize
end

Base.show{K, V}(io::IO, lru::LRU{K, V}) = print(io,"LRU{$K, $V}($(lru.maxsize) $(lru.isfull))")

Base.start(lru::LRU) = start(lru.ht)
Base.next(lru::LRU, state) = next(lru.ht, state)
Base.done(lru::LRU, state) = done(lru.ht, state)

Base.length(lru::LRU) = length(lru.q)
Base.isempty(lru::LRU) = isempty(lru.q)
Base.sizehint!(lru::LRU, n::Integer) = sizehint!(lru.ht, n)

Base.haskey(lru::LRU, key) = haskey(lru.ht, key)
Base.get(lru::LRU, key, default) = haskey(lru, key) ? lru[key] : default

macro get!(lru, key, default)
    quote
        if haskey($(esc(lru)), $(esc(key)))
            value = $(esc(lru))[$(esc(key))]
        else
            value = $(esc(default))
            $(esc(lru))[$(esc(key))] = value
        end
        value
    end
end

function Base.get!{K,V}(default::Base.Callable, lru::LRU{K, V}, key::K)
    if haskey(lru, key)
        return lru[key]
    else
        value = default()
        lru[key] = value
        return value
    end
end

function Base.get!{K,V}(lru::LRU{K,V}, key::K, default::V)
    if haskey(lru, key)
        return lru[key]
    else
        lru[key] = default
        return default
    end
end

function Base.getindex(lru::LRU, key)
    node = lru.ht[key]
    move_to_front!(lru.q, node)
    return node.v
end

function Base.setindex!{K, V}(lru::LRU{K, V}, v, key)
    if haskey(lru, key)
        item = lru.ht[key]
        item.v = v
        move_to_front!(lru.q, item)
    else
        item = LRUNode{K, V}(key, v)
        unshift!(lru.q, item)
        lru.ht[key] = item
    end

    while lru.isfull(lru)
        rm = last(lru.q)
        delete!(lru, rm.k)
    end

    return lru
end

function Base.resize!(lru::LRU, n::Int)
    n < 0 && error("size must be a positive integer")
    lru.maxsize = n
    while lru.isfull(lru)
        rm = last(lru.q)
        delete!(lru, rm.k)
    end
    return lru
end

function Base.delete!(lru::LRU, key; callback::Bool=true)
    item = lru.ht[key]
    callback && lru.cb(item.k, item.v)
    delete!(lru.q, item)
    delete!(lru.ht, key)
    return lru
end

function Base.empty!(lru::LRU; callback::Bool=true)
    if callback
        for item in values(lru.ht)
            lru.cb(item.k, item.v)
        end
    end
    empty!(lru.ht)
    empty!(lru.q)
end

end # module
