MYDB - A Constant Key/Value Database
====================================
- No Ram Copy
- No Eventual*
- Erlang powered
- Alpha Software

Getting started
---------------
1. Change the db.hrl inorder to specify the amounts of buckets and slots/bucket
2. Compile mydb <code>./rebar compile</code>
3. Setup Datastructures: <code>mydb:setup()</code>. This will setup a data and bucket directory inside CWD
4. Start mydb: <code>mydb:start()</code>
5. Use the following commands to play with mydb:

###PUT Request
    
<code>mydb:put(Key, Value) -> ok.                 %% Key :: term(), Value :: term()</code>

<code>mydb:put(Key, Value) -> ok.                 %% Key :: binary(), Value :: binary()</code>

###GET Request
<code>mydb:get(Key) -> Value.                     %% Key :: term(), Value :: term()</code>

<code>mydb:getb(Key) -> Value.                    %% Key :: binary(), Value :: binary()</code>

###UPDATE Request
<code>mydb:update(Key, Value) -> ok.              %% Key :: term(), Value :: term()</code>

<code>mydb:updateb(Key, Value) -> ok.             %% Key :: binary(), Value :: binary()</code>

###DELETE Request
<code>mydb:delete(Key) -> ok.                     %% Key :: term()</code>
<code>mydb:delete(Key) -> ok.                     %% Key :: binary()</code>

###ADD MEMBERSHIP
<code>mydb:add_membership(Group, Key) -> ok.      %% Group :: term(), Key :: term()</code>

###GET MEMBERS
<code>mydb:get_members(Group) -> [{Key, Value}].  %% Group :: term(), Key :: term(), Value :: term()</code>

Testing
-------
Simple Scenario where we enter N times a key value pair where key and value take the String of the current iteration index. For example with N = 1000000: 

Such a scenario can be tested using the following command:

>><code>
    mydb:fill_db_test(N, NrOfWorkers) -> Millis.
</code>

The read performance can be measured with this command:
>><code>
    mydb:read_db_test(N, NrOfWorkers) -> Millis
</code>

You can check the sizes of buckets and data folders: E.g. running the tests above with N == 1000000

>><code>
dergraf:mydb $ du -h data/ buckets/

>>> 27M	data/

>>>9,6M	buckets/
</code>

Basho Benchmark
---------------
Check <code>summary.png</code> to see the results of a bashobench run on my Macbook Pro 2GHz Intel Code i7, SATA HD with 7200 RPM
