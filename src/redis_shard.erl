-module(redis_shard).
-behaviour(gen_server).

%% gen_server callbacks
-export([start_link/1, start_link/2, init/1, handle_call/3,
	     handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% API
-export([pool/1, pool/2, update_shards/1, update_shards/2]).

%% Util funcs
-export([generate_map_and_interval/1, get_matching_pool/3, key_to_index/1]).

-record(state, {interval, map}).

% 2**128 of md5 interval
-define(TOTAL_INTERVAL, 340282366920938463463374607431768211456).

%% API functions
start_link(Servers) ->
    start_link(?MODULE, Servers).

start_link(Name, Servers) when is_atom(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Servers], []).

pool(Arg) ->
    pool(?MODULE, Arg).

pool(Name, Index) when is_integer(Index) ->
    gen_server:call(Name, {pool, Index});
pool(Name, Key) ->
    Index = key_to_index(Key),
    gen_server:call(Name, {pool, Index}).

update_shards(Servers) ->
    update_shards(?MODULE, Servers).
update_shards(Name, Servers) ->
    gen_server:call(Name, {update_shards, Servers}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%% @hidden
%%--------------------------------------------------------------------
init([Servers]) ->
    % add 1 to spread the rem across the interval, the last server in
    % the ring will actually have length(Servers) fewer keys
    {ok, Map, Interval} = generate_map_and_interval(Servers),
	{ok, #state{map=Map, interval=Interval}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% @hidden
%%--------------------------------------------------------------------
handle_call({update_shards, Servers}, _From, State) ->
    {ok, Map, Interval} = generate_map_and_interval(Servers),
    {reply, ok, State#state{map=Map, interval=Interval}};
handle_call({pool, Index}, _From, #state{map=Map, interval=Interval}=State) ->
    {reply, get_matching_pool(Index, Map, Interval), State};

handle_call(_Msg, _From, State) ->
    {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @hidden
%%--------------------------------------------------------------------
terminate(_Reason, _State) -> 
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% @hidden
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
key_to_index(Key) ->
    <<Index:128/big-unsigned-integer>> = crypto:hash(md5, Key),
    Index.

get_matching_pool(Index, Map, Interval) ->
    % Assumption is that the interval is divided evenly among the pools.
    % This means that we don't need to search for the pool, but we can
    % obtain the right key by dividing the Index by the TOTAL_INTERVAL/POOL_SIZE
    % which in turn we roundup to the bigger number and use it as the
    % key in the Map.
    case dict:find(closest_key(Index, Interval), Map) of
        {ok, Value} ->
            Value;
        Error ->
            Error
    end.

closest_key(Index, Interval) ->
    ((Index div Interval) + 1) * Interval.

build_map(Servers, Interval) ->
    build_map(Servers, Interval, 1, dict:new()).

build_map([], _Interval, _Sequence, Acc) ->
    Acc;
build_map([Server|Rest], Interval, Sequence, Acc) ->
    build_map(Rest, Interval, Sequence+1, dict:store(Interval*Sequence, Server, Acc)).

generate_map_and_interval([]) ->
    {ok, dict:new(), ?TOTAL_INTERVAL};
generate_map_and_interval(Servers) ->
    Interval = (?TOTAL_INTERVAL div erlang:length(Servers))+1,
    Map = build_map(Servers, Interval),
    {ok, Map, Interval}.
