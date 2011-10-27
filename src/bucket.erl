-module(bucket).
-include("../include/db.hrl").


-behaviour(gen_server).

%% API
-export([start_link/2, insert/3, lookup/1, delete/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-define(SERVER(Id), list_to_atom("bucket" ++ integer_to_list(Id))).
-define(NORMAL, 0). 
-define(UPDATED, 1).
-define(DELETED, 2).
-record(state, {id, iodevice, table, next_write}).

%%%===================================================================
%%% API
%%%===================================================================
insert(Hash, DataFile, Offset) ->
    BucketId = find_bucket(Hash),
    gen_server:call(?SERVER(BucketId), {bucket_insert, Hash, list_to_integer(DataFile), Offset}).

lookup(Hash) ->
    BucketId = find_bucket(Hash),
    gen_server:call(?SERVER(BucketId), {bucket_lookup, Hash}).

delete(Hash) ->
    BucketId = find_bucket(Hash),
    gen_server:call(?SERVER(BucketId), {bucket_delete, Hash}).

start_link(BucketId, FileName) ->
    gen_server:start_link({local, ?SERVER(BucketId)}, ?MODULE, [BucketId, FileName], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([BucketId, FileName]) ->
    {ok, IoDevice} = file:open(FileName, [append, write, read, raw, binary]),
    Tid = ets:new(slot_table, [set]),
    {ok, #state{id=BucketId, iodevice=IoDevice, table=Tid, next_write=[]}, 0}.

handle_call({bucket_insert, Hash, DataFile, DataOffset}, _From, #state{iodevice=BucketFile,table=Tid, next_write=NextOffsets} = State) ->
    HashTable = find_hash_table(Hash),
    {BucketOffset, NewNextOffsets} = get_next_offset(BucketFile, NextOffsets),
    Bytes = write(Hash, DataFile, DataOffset, BucketFile, BucketOffset),
    ok = hash_table_insert(Tid, HashTable, Hash, BucketOffset, Bytes),
    {reply, ok, State#state{next_write=NewNextOffsets}};

handle_call({bucket_lookup, Hash}, _From, #state{table=Tid} = State) ->
    HashTable = find_hash_table(Hash),
    Entries = hash_table_lookup(Tid, HashTable, ?NORMAL, Hash),
    Reply = [{DataFile,DataFileOffset} || {_,DataFile,DataFileOffset} <- Entries],
    {reply, Reply, State};

handle_call({bucket_delete, Hash}, _From, #state{iodevice=IoDevice, table=Tid,next_write=NextOffsets } = State) ->
    HashTable = find_hash_table(Hash),
    Entries = hash_table_lookup(Tid, HashTable, ?NORMAL, Hash),
    NewNextOffsets = delete(IoDevice, Entries, []),
    ok = hash_table_delete(Tid, HashTable, Hash),
    {reply, {deleted, length(Entries)}, State#state{next_write=NewNextOffsets ++ NextOffsets}}.

handle_cast(_Req, State) ->   
    {noreply, State}.

handle_info(timeout, #state{id=Id, iodevice=IoDev, table=Tid} = State) ->
    NextOffsets = init_bucket(Tid, IoDev),
    io:format("Slot table ~p initialized~n", [Id]),
    {noreply, State#state{next_write=NextOffsets}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_bucket(Tid, BucketFile) ->
    init_hash_tables(Tid, BucketFile, 0, []).

init_hash_tables(Tid, BucketFile, Offset, NextOffsets) ->
    case file:pread(BucketFile, Offset, 10) of
        {ok, Bytes} ->
            <<Hash:32, Status:8, _Rest:40>> = Bytes,
            NewNextOffsets = case Status of
                ?DELETED ->
                    [Offset] ++ NextOffsets;
                _ ->
                    HashTable = find_hash_table(Hash),
                    hash_table_insert(Tid, HashTable, Hash, Offset, Bytes),
                    NextOffsets
            end,
            init_hash_tables(Tid, BucketFile, Offset + 10, NewNextOffsets);
        {error, Reason} ->
            io:format("Cant read-in bucket file entries ~p~n", [Reason]);
        eof ->
            NextOffsets
    end.

find_bucket(Hash) ->
    Hash rem ?NR_OF_BUCKETS.    

find_hash_table(Hash) ->
    (Hash div ?NR_OF_BUCKETS) rem ?NR_OF_SLOTS.

get_next_offset(_BucketFile, [Offset|T]) -> {Offset, T};
get_next_offset(BucketFile, []) -> 
    {ok, EOF} = file:position(BucketFile, eof),
    {EOF, []}.

write(Hash, DataFile, DataOffset, BucketFile, BucketFileOffset) ->
    Bytes =  <<Hash:32, ?NORMAL:8, DataFile:8, DataOffset:32>>,
    ok = file:pwrite(BucketFile, BucketFileOffset, Bytes),
    Bytes.

delete(_, [], Acc) -> Acc;
delete(F, [{undefined,_,_}|T], Acc) ->
    delete(F, T, Acc);
delete(BucketFile, [{BucketFileOffset,_,_} | T], Acc) ->
    ok = file:pwrite(BucketFile, BucketFileOffset + 4, <<?DELETED:8>>),
    delete(BucketFile, T, [BucketFileOffset] ++ Acc).

hash_table_delete(Tid, HashTable, Hash) ->
    true = ets:delete(Tid, {HashTable, ?NORMAL, Hash}),
    ok.

hash_table_insert(Tid, HashTable, Hash, BucketFileOffset, 
        <<Hash:32, Status:8, DataFile:8, DataFileOffset:32>>) ->
        case hash_table_lookup(Tid, HashTable, Status, Hash) of
            [] ->
                true = ets:insert(Tid, {{HashTable, Status, Hash}, 
                        [{BucketFileOffset, integer_to_list(DataFile), DataFileOffset}]});
            Entries ->
                %% hash collision, not yet sure if we want to append the collided entry
                true = ets:insert(Tid, {{HashTable, Status, Hash}, 
                        [{BucketFileOffset, integer_to_list(DataFile), DataFileOffset}] ++ Entries})
        end,            
        ok.

    hash_table_lookup(Tid, HashTable, Status, Hash) ->
        case ets:lookup(Tid, {HashTable, Status, Hash}) of
            [{{_,_,_}, Entries}] ->
                Entries;
            [] ->
                []
        end.
