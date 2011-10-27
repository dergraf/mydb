-module(data_file).

-behaviour(gen_server).

%% API
-export([start_link/2, write/3, update/4, read/2, delete/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-define(SERVER(FileName), list_to_atom("data" ++ FileName)).
-define(NORMAL, 0). 
-define(UPDATED, 1).
-define(DELETED, 2).

-record(state, {name, iodevice}).

%%%===================================================================
%%% API
%%%===================================================================
write(FileName, Key, Value) ->
    gen_server:call(?SERVER(FileName), {write, {Key, Value}}).

update(FileName, Key, NewValue, Offset) ->
    gen_server:call(?SERVER(FileName), {update, {Key, NewValue, Offset}}).

read(FileName, Offset) ->
    gen_server:call(?SERVER(FileName), {read, Offset}).

delete(FileName, Key, Offset) ->
    gen_server:call(?SERVER(FileName), {delete, {Key, Offset}}).

start_link(Dir, FileName) ->
    gen_server:start_link({local, ?SERVER(FileName)}, ?MODULE, [Dir ++ FileName], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([FileName]) ->
    {ok, IoDevice} = file:open(FileName, [delayed_write, append, write, read, raw, binary]),
    {ok, #state{name=FileName, iodevice=IoDevice}}.

handle_call({write, KVPair}, _From, #state{iodevice=IoDev} = State) ->
    Reply = write_data(IoDev, KVPair),
    {reply, Reply, State};

handle_call({read, Offset}, _From, #state{iodevice=IoDev} = State) ->
    Reply = read_data(IoDev, Offset),
    {reply, Reply, State};

handle_call({update, {Key, NewValue, Offset}}, _From, #state{iodevice=IoDev} = State) ->
    Reply = update_data(IoDev, Key, NewValue, Offset),
    {reply, Reply, State};

handle_call({delete, {Key, Offset}}, _From, #state{iodevice=IoDev} = State) ->
    Reply = delete_data(IoDev, Key, Offset),
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
read_data(IoDevice, Offset) ->
    {ok, <<KeySize:32, ValSize:32, Status:8>>} = file:pread(IoDevice, Offset, 9),
    case Status of
        ?DELETED ->
            [];
        _ ->
            {ok, <<Key:KeySize/binary, Val:ValSize/binary>>} = file:pread(IoDevice, Offset + 9, KeySize + ValSize),
            {Key, Val}
    end.

update_data(IoDevice, Key, NewValue, Offset) ->
    {ok, <<KeySize:32, ValSize:32, _Status:8>>} = file:pread(IoDevice, Offset, 9),
    OldSize = KeySize + ValSize + 9,
    NewValSize = byte_size(NewValue),
    NewSize = KeySize + NewValSize + 9,
    case NewValSize =< ValSize of
        true -> %% inline update
            ok = file:pwrite(IoDevice, Offset, <<KeySize:32, NewValSize:32, ?NORMAL:8, Key/binary, NewValue/binary>>),
            {inline, {Offset, OldSize, NewSize}};
        false -> %% can't inline update
            %% mark as deleted
            ok = file:pwrite(IoDevice, Offset + 8, <<?DELETED:8>>),
            %% append new entry
            {NewOffset, NewSize} = write(IoDevice, Key, NewValue),
            {append, {NewOffset, OldSize, NewSize}}
    end.

delete_data(IoDevice, Key, Offset) ->
    {ok, <<KeySize:32, ValSize:32, Status:8>>} = file:pread(IoDevice, Offset, 9),
    %% extract key in order to check that we are deleting the proper entry (hash collision)
    {ok, <<StoredKey:KeySize/binary>>} = file:pread(IoDevice, Offset + 9, KeySize),
    NrOfBytes = KeySize + ValSize + 9,
    case StoredKey == Key of
        true ->
            case Status of
                ?DELETED ->
                    {already_deleted, Offset, NrOfBytes};
                ?NORMAL ->
                    ok = file:pwrite(IoDevice, Offset + 8, <<?DELETED:8>>),
                    {deleted, Offset, NrOfBytes};
                _ ->
                    {cant_delete, Offset, NrOfBytes}
            end;
        false ->  
            %% already deleted by another worker
            %% we may check here the status of the new entry
            {already_deleted, Offset, NrOfBytes}
    end.

write_data(IoDevice, {Key, Value}) -> 
    {ok, EOF} = file:position(IoDevice, eof),
    KeySize = byte_size(Key),
    ValSize = byte_size(Value),
    ok = file:write(IoDevice, <<KeySize:32, ValSize:32, ?NORMAL:8, Key/binary, Value/binary>>), %% append mode
    {EOF, ValSize + KeySize + 9}.
