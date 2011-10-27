
-module(mydb_sup).

-behaviour(supervisor).
-include("../include/db.hrl").


%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(DB(Id, Args), {{mydb, Id}, {mydb, start_link, Args}, permanent, 5000, worker, [mydb]}).
-define(DATA(Id), {{data_file, Id}, {data_file, start_link, ["data/", Id]}, permanent, 5000, worker, [data_file]}).
-define(BUCKET(Id, Args), {{bucket, Id}, {bucket, start_link, Args}, permanent, 5000, worker, [bucket]}).



%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {DataFileChildren, DbChildren} =
        case file:list_dir("data") of
            {ok, DataFiles} ->
                {[ ?DATA(D) || D <- DataFiles],
                [?DB(Id, [Id, DataFiles])|| Id <- lists:seq(1, ?NR_OF_BUCKETS)]};
            {error, enoent} -> %% no dir available
                error(no_data_dir);
            {error, R1} ->
                error(R1)
    end,

    BucketChildren = 
        case file:list_dir("buckets") of
            {ok, BucketFiles} ->
                ?NR_OF_BUCKETS = length(BucketFiles),
                [ ?BUCKET(F, [list_to_integer(F), "buckets/"++F]) 
                  || F <- BucketFiles];
            {error, enoent} -> %% no dir available
                error(no_bucket_dir);
            {error, R} ->
                error(R)
        end,
    {ok, {{one_for_one, 5, 10}, DbChildren ++ DataFileChildren ++ BucketChildren}}.

