const DEFAULT_NAME = "lmdb"

mutable struct ThreadSafePersistentDict{K,V} <: AbstractDict{K,V} 
    env::Environment
    rotxn::Vector{Transaction}
    wlock::ReentrantLock
    function ThreadSafePersistentDict{K,V}(path; mapsize=10485760, maxreaders=Threads.nthreads()) where {K,V}
        env, rotxn = create(), Vector{Transaction}()
        try
            env[:MapSize] = mapsize
            open(env, path, flags=Cuint(LMDB.NOSUBDIR))
            for i in 1:Threads.nthreads()
                # allocate tranaction handle for each thread
                txn = start(env, flags=(Cuint(LMDB.RDONLY) | Cuint(LMDB.NOTLS)))
                reset(txn)  # close the transaction but keep the handle it can be renewed later
                push!(rotxn, txn)
            end
        catch e
            for txn in rotxn
                abort(txn)
            end
            close(env)
            rethrow(e)
        end
        dict = new{K,V}(env, rotxn, ReentrantLock())
        finalizer(close, dict)
        return dict
    return 
    end
end

function Base.show(io::IO, d::ThreadSafePersistentDict)
    n = length(d)
    n > 0 || return print(io, typeof(d), "()")
    return print(io, typeof(d), " with ", length(d), " entries")
end

function Base.getindex(dict::ThreadSafePersistentDict{K,V}, key::K) where {K,V}
    val = get(dict, key, nothing)
    isnothing(val) == false || throw(KeyError(key))
    return val
end

function Base.get(dict::ThreadSafePersistentDict{K,V}, key::K, default) where {K,V}
    NOTFOUND::Cint = -30798
    rotxn = dict.rotxn[Threads.threadid()]
    try
        renew(rotxn)
        return get(rotxn, open(rotxn), key, V)
    catch e
        if e isa LMDBError && e.code == NOTFOUND # MDB_NOTFOUND
            return default
        end
        rethrow(e)
    finally
        reset(rotxn)
    end
end

function Base.setindex!(dict::ThreadSafePersistentDict{K,V}, val::V, key::K) where {K,V}
    txn = nothing
    lock(dict.wlock)
    try
        txn = start(dict.env, flags=Cuint(LMDB.NOLOCK))
        put!(txn, open(txn), key, val)
        commit(txn)
    catch e
        isnothing(txn) == false || abort(txn)
        rethrow(e)
    finally
        unlock(dict.wlock)
    end  
    return val
end

mutable struct PairsIterator{K,V}
   txn::Transaction
   cur::Cursor
   function PairsIterator(::Type{K}, ::Type{V}, env::Environment) where {K,V}
        txn = start(env, flags=(Cuint(LMDB.RDONLY) | Cuint(LMDB.NOTLS)))
        try
            iter = new{K,V}(txn, open(txn, open(txn)))
            finalizer(close, iter)  # close lazily
            return iter
        catch e
            abort(txn)
            rethrow(e)
        end
   end
end

function Base.close(iter::PairsIterator)
    isopen(iter.cur) == false || close(iter.cur)
    isopen(iter.txn) == false || abort(iter.txn)
end

function mdb_cursor_get!(iter::PairsIterator, op::Cint)
    NOTFOUND::Cint = -30798
    mdb_key_ref = Ref(MDBValue())
    mdb_val_ref = Ref(MDBValue())
    ret = ccall((:mdb_cursor_get, liblmdb), Cint, (Ptr{Nothing}, Ptr{MDBValue}, Ptr{MDBValue}, Cint),
                iter.cur.handle, mdb_key_ref, mdb_val_ref, op)
    iszero(ret) == false || return (mdb_key_ref[], mdb_val_ref[])
    if ret == NOTFOUND
        close(iter)  # eagerly close
        return nothing
    elseif !iszero(ret)
        throw(LMDBError(ret))
    end

end

function Base.iterate(iter::PairsIterator{K,Nothing}, first=true) where {K}
    kv = mdb_cursor_get!(iter, Cint(first ? FIRST : NEXT))
    return isnothing(kv) ? nothing : (convert(K, Ref(kv[1])), false)
end

function Base.iterate(iter::PairsIterator{Nothing,V}, first=true) where {V}
    kv = mdb_cursor_get!(iter, Cint(first ? FIRST : NEXT))
    return isnothing(kv) ? nothing : (convert(V, Ref(kv[2])), false)
end

function Base.iterate(iter::PairsIterator{K,V}, first=true) where {K,V}
    kv = mdb_cursor_get!(iter, Cint(first ? FIRST : NEXT))
    return isnothing(kv) ? nothing : (convert(K, Ref(kv[1])) => convert(V, Ref(kv[2])), false)
end

Base.IteratorSize(::PairsIterator) = Base.SizeUnknown() # might change during iteration so we do not return static length
Base.IteratorSize(::ThreadSafePersistentDict) = Base.SizeUnknown() # might change during iteration so we do not return static length
Base.eltype(iter::PairsIterator{K,Nothing}) where {K,V} = K
Base.eltype(iter::PairsIterator{Nothing,V}) where {K,V} = V
Base.eltype(iter::PairsIterator{K,V}) where {K,V} = Pair{K,V}

Base.keys(dict::ThreadSafePersistentDict{K,V}) where {K,V} = PairsIterator(K, Nothing, dict.env)
Base.values(dict::ThreadSafePersistentDict{K,V}) where {K,V} = PairsIterator(Nothing, V, dict.env)
function Base.iterate(dict::ThreadSafePersistentDict{K,V}, state=nothing) where {K,V}
    iter = isnothing(state) ? PairsIterator(K, V, dict.env) : state
    next = iterate(iter, isnothing(state))
    return isnothing(next) ? nothing : (next[1], iter)
end

function Base.length(dict::ThreadSafePersistentDict)
    txn = dict.rotxn[Threads.threadid()]
    try
        renew(txn)
        return convert(Int, dbstat(txn, open(txn)).entries)
    finally
        reset(txn)
    end
end

function Base.close(dict::ThreadSafePersistentDict)
    for txn in dict.rotxn
        isopen(txn) == false || abort(txn)
    end    
    isopen(dict.env) == false || close(dict.env)
end
