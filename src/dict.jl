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
    rotxn = dict.rotxn[Threads.threadid()]
    try
        renew(rotxn)
        return get(rotxn, open(rotxn), key, V)
    catch e
        if e isa LMDBError && e.code == -30798 # MDB_NOTFOUND
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

"Iterate over key/values"
function _iterate(cur::Cursor, op, ::Type{K}, ::Type{V}) where {K,V}
    # Setup parameters
    mdb_key_ref = Ref(MDBValue())
    mdb_val_ref = Ref(MDBValue())
    ret = ccall( (:mdb_cursor_get, liblmdb), Cint,
               (Ptr{Nothing}, Ptr{MDBValue}, Ptr{MDBValue}, Cint),
                cur.handle, mdb_key_ref, mdb_val_ref, Cint(op))
    if ret == 0
        # Convert to proper type
        return convert(K, mdb_key_ref) => convert(V, mdb_val_ref)
    else
        ret != -30798 || return nothing
        throw(LMDBError(ret))
    end
end

function Base.iterate(dict::ThreadSafePersistentDict{K,V}) where {K,V}
    txn = start(dict.env, flags=Cuint(LMDB.RDONLY))
    cur = Cursor(C_NULL)
    cleanup = true
    try
        cur = open(txn, open(txn))
        val = _iterate(cur, FIRST, K, V)
        if !isnothing(val)
            finalizer(cur) do c
                c.handle != C_NULL || return
                let txn = transaction(c)
                    close(c)
                    abort(txn)
                end
            end
            cleanup = false
            return (val, cur) # NOTE: cursor and txn still open
        end
    catch e
        rethrow(e)
    finally
        if cleanup
            cur.handle == C_NULL || close(cur)
            abort(txn)
        end
    end
    return nothing
end
function Base.iterate(::ThreadSafePersistentDict{K,V}, cur::Cursor) where {K,V}
    val = _iterate(cur, NEXT, K, V)
    isnothing(val) || return (val, cur)
    let txn = transaction(cur)
        close(cur)
        abort(txn)
    end
    return nothing
end

function Base.length(dict::ThreadSafePersistentDict)
    txn = start(dict.env, flags=Cuint(LMDB.RDONLY))
    try
        return Int(dbstat(txn, open(txn)).entries)
    finally
        abort(txn)
    end
end

function Base.close(dict::ThreadSafePersistentDict)
    for txn in dict.rotxn
        isopen(txn) == false || abort(txn)
    end    
    isopen(dict.env) == false || close(dict.env)
end
