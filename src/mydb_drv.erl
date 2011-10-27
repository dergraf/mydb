-module(mydb_drv).
-export([foo/1, bar/1]).
-on_load(init/0).
init() ->
    ok = erlang:load_nif("./priv/mydb_drv", 0).
foo(_X) ->
    exit(nif_library_not_loaded).
bar(_Y) ->
    exit(nif_library_not_loaded).
