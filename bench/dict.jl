import Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using LMDB
using BenchmarkTools

const TESTDB = "test.lmdb"

function bench_read(n, ks, vs; name=TESTDB)
    @info "== Randomly reading $n different keys with key size $ks bytes and data size $vs bytes =="
    keys = [rand(UInt8, ks) for _ in 1:n]
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(TESTDB, mapsize=1073741824)
    try
        for k in keys
            dict[k] = rand(UInt8, vs)
        end
        Threads.@threads for _ in 1:Threads.nthreads()
            @btime ($dict[key]) setup=(key=rand($keys))
        end
    finally
        close(dict)
    end
end

function bench_write(n, ks, vs; name=TESTDB)
    @info "== Randomly writing/updating $n different keys with key size $ks bytes and data size $vs bytes =="
    keys = [rand(UInt8, ks) for _ in 1:n]
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(TESTDB, mapsize=1073741824)
    try
        @btime ($dict[key] = val) setup=(key=rand($keys); val=rand(UInt8, $vs))
    finally
        close(dict)
    end
end

function bench_collect(n, ks, vs; name=TESTDB)
    @info "== Collecting db with $n different keys =="
    keys = [rand(UInt8, ks) for _ in 1:n]
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(TESTDB, mapsize=1073741824)
    try
        for k in keys
            dict[k] = rand(UInt8, vs)
        end
        @btime collect($dict);
    finally
        close(dict)
    end
end

function bench_length(n, ks, vs; name=TESTDB)
    @info "== length of db with $n different keys =="
    keys = [rand(UInt8, ks) for _ in 1:n]
    rm(name, force=true, recursive=true)
    dict = ThreadSafePersistentDict{Vector{UInt8},Vector{UInt8}}(TESTDB, mapsize=1073741824)
    try
        for k in keys
            dict[k] = rand(UInt8, vs)
        end
        @btime length($dict);
    finally
        close(dict)
    end
end

bench_length(1000, 64, 1024)
bench_collect(1000, 64, 1024)
bench_write(1000, 64, 1024)
bench_read(1000, 64, 1024)

bench_length(10000, 64, 1024)
bench_collect(10000, 64, 1024)
bench_write(10000, 64, 1024)
bench_read(10000, 64, 1024)
