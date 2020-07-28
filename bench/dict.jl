import Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using LMDB
using BenchmarkTools

const TESTDB = "test.lmdb"

function bench_read(n, ks, vs; name=TESTDB)
    @info "== Randomly reading $n different keys with key size $ks bytes and data size $vs bytes =="
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(name, mapsize=1073741824)
    randkeys = [rand(UInt8, ks) for _ in 1:n]
    try
        for k in randkeys
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime ($dict[key]) setup=(key=rand($randkeys))
        end
    finally
        close(dict)
    end
end

function bench_write(n, ks, vs; name=TESTDB)
    @info "== Randomly writing/updating $n different keys with key size $ks bytes and data size $vs bytes =="
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(name, mapsize=1073741824)
    randkeys = [rand(UInt8, ks) for _ in 1:n]
    try
        for k in randkeys
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime ($dict[key] = val) setup=(key=rand($randkeys); val=rand(UInt8, $vs))
        end        
    finally
        close(dict)
    end
end

function bench_collect(n, ks, vs; name=TESTDB)
    @info "== Collecting db with $n different keys =="
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(name, mapsize=1073741824)
    try
        for k in [rand(UInt8, ks) for _ in 1:n]
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime collect($dict);
        end
    finally
        close(dict)
    end
end

function bench_keys(n, ks, vs; name=TESTDB)
    @info "== Iterating through all $n different keys =="
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(name, mapsize=1073741824)
    try
        for k in [rand(UInt8, ks) for _ in 1:n]
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime collect(keys($dict));
        end
    finally
        close(dict)
    end
end

function bench_length(n, ks, vs; name=TESTDB)
    @info "== length of db with $n different keys =="
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(name, mapsize=1073741824)
    try
        for k in [rand(UInt8, ks) for _ in 1:n]
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime length($dict);
        end
    finally
        close(dict)
    end
end

bench_length(1000, 64, 1024)
bench_keys(1000, 64, 1024)
bench_collect(1000, 64, 1024)
bench_write(1000, 64, 1024)
bench_read(1000, 64, 1024)

bench_length(10000, 64, 1024)
bench_keys(10000, 64, 1024)
bench_collect(10000, 64, 1024)
bench_write(10000, 64, 1024)
bench_read(10000, 64, 1024)
