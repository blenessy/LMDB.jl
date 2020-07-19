module LMDB_Dict
    using LMDB
    using Test
    
    rm("LMDB_Dict", force=true)
    dict = ThreadSafePersistentDict{String,String}("test.lmdb")

    # default works if key is not found
    @test get(dict, "foo", "bar") == "bar"

    # KeyError is thrown if key is not found
    @test_throws KeyError dict["foo"]

    # write key
    @test (dict["foo"] = "baz") == "baz"
    
    @test dict["foo"] == "baz"
    @test get(dict, "foo", "bar") == "baz"

    @test length(dict) == 1

    dict["bar"] = "baz"
    @test collect(dict) == ["bar" => "baz", "foo" => "baz"]

    #dict = ThreadSafePersistentDict("foo" => 1, "bar" => 2)
end