-module(mydb).
-include("../include/db.hrl").

-behaviour(gen_server).

%% API
-export([start/0,
        start_link/2, 
        setup/0, 
        put/2,
        putb/2,
        delete/1,
        deleteb/1,
        get/1,
        getb/1,
        update/2, 
        updateb/2,
        add_membership/2, 
        get_members/1]).

%% gen_server callbacks
-export([init/1, 
        handle_call/3, 
        handle_cast/2, 
        handle_info/2,
        terminate/2, 
        code_change/3]).

-export([fill_db/2, fill_db/3, fill_db_test/2]).
-export([read_db/2, read_db/3, read_db_test/2]).

-define(SERVER(Id), list_to_atom("db" ++ integer_to_list(Id))).
-define(SERVER, ?SERVER(random:uniform(?NR_OF_BUCKETS))).

-record(state, {table=compact, files=[]}).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    application:start(mydb).

start_link(Id, DataFiles) ->
    gen_server:start_link({local, ?SERVER(Id)}, ?MODULE, [DataFiles],[]).

put(Key, Value) ->
    putb(term_to_binary(Key), term_to_binary(Value)).

putb(Key, Value) ->
    gen_server:call(?SERVER, {put, {Key, Value}}).

delete(Key) ->
    deleteb(term_to_binary(Key)).

deleteb(Key) ->
    gen_server:call(?SERVER, {delete, Key}).  

update(Key, Value) ->
    updateb(term_to_binary(Key), term_to_binary(Value)).

updateb(Key, Value) ->
    gen_server:call(?SERVER, {update, {Key, Value}}).

get(Key) ->
    Values = getb(term_to_binary(Key)),
    [binary_to_term(V) || V <- Values].

getb(Key) ->
    gen_server:call(?SERVER, {get, Key}).

add_membership(GroupKey, Key) ->
    gen_server:call(?SERVER, {add_membership, {term_to_binary({group, GroupKey}), term_to_binary(Key)}}).

get_members(GroupKey) ->
    Values = gen_server:call(?SERVER, {get_members, term_to_binary({group, GroupKey})}),
    [{binary_to_term(K), binary_to_term(V)} || {K,V} <- Values].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([DataFiles]) ->
    case lists:member(compact, ets:all()) of
        true -> 
            ok;
        false ->
            ets:new(compact, [named_table, ordered_set, public])
    end,            
    {ok, #state{files=DataFiles}}.

handle_call({put, {Key, Value}}, _From, #state{table=Tid, files=Files} = State) ->
    ok = write(Tid, Key, Value, lists:last(Files)),
    {reply, ok, State};

handle_call({get, Key}, _From, State) ->
    Values = read(Key),
    Reply = [V || {K,V} <- Values, K == Key],
    {reply, Reply, State};

handle_call({update, {Key, Value}}, _From, #state{table=Tid, files=Files} = State) ->
    ok = update(Tid, lists:last(Files), Key, Value),
    {reply, ok, State};

handle_call({delete, Key}, _From, #state{table=Tid} = State) ->
    ok = delete(Tid, Key),
    {reply, ok, State};

handle_call({add_membership, {GroupKey, Key}}, _From, State) ->
    {Offset, _NrOfBytes} = data_file:write(GroupKey, Key),
    GroupKeyHash = hash(GroupKey),
    ok = bucket:insert(GroupKeyHash, Offset),
    {reply, ok, State};

handle_call({get_members, GroupKey}, _From, State) ->
    Members = [V || {K,V} <-  read(GroupKey), K == GroupKey],
    Reply = read(Members),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

hole_open(Tid, DataFile, Offset, NrOfBytes) ->
    ets:insert_new(Tid, {{NrOfBytes, DataFile, Offset}}).

hole_close(Tid, DataFile, Offset, NrOfBytes) ->
    ets:delete(Tid, {NrOfBytes, DataFile, Offset}).

hole_available(Tid, NrOfBytes) ->    
    MatchSpec = [{{{'$1', '$2', '$3'}}, [{'>=', '$1', NrOfBytes}], ['$$']}],
    case ets:select(Tid, MatchSpec, 1) of
        {[[AvailableNrOfBytes, DataFile, Offset]], _Cont} ->
            hole_close(Tid, DataFile, Offset, AvailableNrOfBytes),
            {DataFile, Offset};
        '$end_of_table' ->
            none
    end.    

update(Tid, CurrentDataFile, Key, Value) ->
    Hash = hash(Key),
    ListOfOffsets = bucket:lookup(Hash),
    update(Tid, CurrentDataFile, Key, Value, ListOfOffsets).

update(Tid, CurrentDataFile, Key, Value, [{_, DataFile, Offset}|T]) ->
    case inline_update(Tid, DataFile, Offset, Key, Value) of
        false ->
            ok = write(Key, Value, CurrentDataFile);
        _ -> 
            ok
    end,           
    update(Tid, CurrentDataFile, Key, Value, T);
update(_,_,_,_,[]) -> ok.


inline_update(Tid, DataFile, Offset, Key, Value) ->
    case data_file:update(DataFile, Key, Value, Offset) of
        {inline, {Offset, OldSize, NewSize}} ->
            case NewSize < OldSize of
                true ->
                    hole_open(Tid, DataFile, Offset + NewSize, OldSize - NewSize),
                    ok;
                false ->
                    ok %% match
            end;
        {false, _NrOfBytes} ->
            false
    end.

write(Key, Value, DataFile) ->
    {Offset, _NrOfBytes} = data_file:write(DataFile, Key, Value),
    ok = bucket:insert(hash(Key), DataFile, Offset).

write(Tid, Key, Value, CurrentDataFile) ->
    NrOfBytes = byte_size(Key) + byte_size(Value) + 9,
    case hole_available(Tid, NrOfBytes) of
        {DataFile, Offset} ->
            ok = inline_update(Tid, DataFile, Offset, Key, Value),
            ok = bucket:insert(hash(Key), DataFile, Offset);
        none ->
            ok = write(Key, Value, CurrentDataFile)
    end.

delete(Tid, Key) ->
    Hash = hash(Key),
    ListOfOffsets = bucket:lookup(Hash),
    delete(Tid, Hash, Key, ListOfOffsets).

delete(_,_,_,[]) -> ok;
delete(Tid, Hash, Key, [{DataFile, Offset}|T]) ->
    case data_file:delete(DataFile, Key, Offset) of
        {deleted, Offset, NrOfBytes} ->
            {deleted, _NrOfDeletedEntries} = bucket:delete(Hash),
            hole_open(Tid, DataFile, Offset, NrOfBytes),
            delete(Tid, Hash, Key, T);
        {already_deleted, _, _} ->
            ok;
        {cant_delete, _, _} ->
            %% unknown status
            ok
    end,
    delete(Tid, Hash, Key, T).

read(Key) ->
    Hash = hash(Key),
    ListOfOffsets = bucket:lookup(Hash),
    read(ListOfOffsets, []).

read([{DataFile, Offset}|T], Acc) ->
    KVPair = data_file:read(DataFile, Offset),
    read(T, [KVPair] ++ Acc);
read([], Acc) -> Acc.

hash(Key) ->
    erlang:phash2(Key).


%%%===================================================================
%%% Setup functions
%%%===================================================================
setup() ->
    ok = file:make_dir("buckets"),
    ok = file:make_dir("data"),
    [create_file("buckets/" ++ integer_to_list(Nr)) || Nr <- lists:seq(0,?NR_OF_BUCKETS - 1)], 
    create_file("data/0").

create_file(FileName) ->
    file:write_file(FileName, <<>>).

%%%===================================================================
%%% Test functions
%%%===================================================================
fill_db_test(NrOfInserts, NrOfWorkers) ->
    NrOfInsertsPerWorker = NrOfInserts div NrOfWorkers,
    spawn_write_workers(NrOfWorkers, self(), 0, NrOfInsertsPerWorker),
    Times = receive_results(NrOfWorkers, []),
    lists:sum(Times) div NrOfWorkers div 1000. %millis

read_db_test(NrOfInserts, NrOfWorkers) ->
    NrOfInsertsPerWorker = NrOfInserts div NrOfWorkers,
    spawn_read_workers(NrOfWorkers, self(), 0, NrOfInsertsPerWorker),
    Times = receive_results(NrOfWorkers, []),
    lists:sum(Times) div NrOfWorkers div 1000. %millis

receive_results(NrOfResults, Acc) when NrOfResults > 0->
    receive
        {result, Time} ->
            receive_results(NrOfResults -1, [Time] ++ Acc); 
        _ ->
            failed
    end;
receive_results(_, Acc) -> Acc.

spawn_write_workers(NrOfWorkers, Pid, From, Add) when NrOfWorkers > 0->
    spawn(?MODULE, fill_db, [Pid, From, From + Add]),
    spawn_write_workers(NrOfWorkers -1, Pid, From + Add, Add); 
spawn_write_workers(_,_,_,_) -> ok.

spawn_read_workers(NrOfWorkers, Pid, From, Add) when NrOfWorkers > 0->
    spawn(?MODULE, read_db, [Pid, From, From + Add]),
    spawn_read_workers(NrOfWorkers -1, Pid, From + Add, Add); 
spawn_read_workers(_,_,_,_) -> ok.

fill_db(Pid, From, To) ->
    {Time, ok} = timer:tc(?MODULE, fill_db, [From, To]),
    Pid ! {result, Time}.

fill_db(From, To) when To > From ->
    K = integer_to_list(To),
    mydb:put(K,K),
    fill_db(From, To -1);
fill_db(_From, _To) -> ok.

read_db(Pid, From, To) ->
    {Time, ok} = timer:tc(?MODULE, read_db, [From, To]),
    Pid ! {result, Time}.

read_db(From, To) when To > From ->
    K = integer_to_list(To),
    [K] = mydb:get(K),
    read_db(From, To -1);
read_db(_From, _To) -> ok.
