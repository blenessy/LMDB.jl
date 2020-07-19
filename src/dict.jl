const DEFAULT_NAME = "lmdb"

mutable struct ThreadSafePersistentDict{K,V} <: AbstractDict{K,V} 
    env::Environment
    function ThreadSafePersistentDict{K,V}(path; mapsize=10485760, maxreaders=Threads.nthreads()) where {K,V}
        env = create()
        env[:MapSize] = mapsize
        flags = Cuint(LMDB.NOSUBDIR)
        open(env, path, flags=flags)
        finalizer(env) do env
            env.handle == C_NULL || close(env)
        end
        return new{K,V}(env)
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
    txn = start(dict.env, flags=Cuint(LMDB.RDONLY))
    try
        return get(txn, open(txn), key, V)
    catch e
        if e isa LMDBError && e.code == -30798 # MDB_NOTFOUND
            return default
        end
        rethrow(e)
    finally
        abort(txn)
    end
end

function Base.setindex!(dict::ThreadSafePersistentDict{K,V}, val::V, key::K) where {K,V}
    txn = start(dict.env)
    try
        put!(txn, open(txn), key, val)
        commit(txn)
    catch e
        abort(txn)
        rethrow(e)
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

Base.close(dict::ThreadSafePersistentDict) = close(dict.env)
