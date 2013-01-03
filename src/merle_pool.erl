-module(merle_pool).

%% Basically the same functionality than pg2,  but process groups are local rather than global.
-export([create/1, delete/1, join/2, leave/2, 
    get_members/1, count_available/1, clean_locks/0,
    get_client/2,
    which_groups/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(PIDS_TABLE, merle_pool_pids).
-define(INDICES_TABLE, merle_pool_indices).

-define(CLEAN_LOCKS_INTERVAL, 10000). % every 10 seconds

-record(server_state, {
    periodic_lock_clean
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(Name) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            gen_server:call(?MODULE, {create, Name});
        _ ->
            ok
    end.
    
delete(Name) ->
    gen_server:call(?MODULE, {delete, Name}).

join(Name, Pid) when is_pid(Pid) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {error, {no_such_group, Name}};
        _ ->
            gen_server:call(?MODULE, {join, Name, Pid})
    end.
    
leave(Name, Pid) when is_pid(Pid) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {error, {no_such_group, Name}};
        _ ->
            gen_server:call(?MODULE, {leave, Name, Pid})
    end.

get_members(Name) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] -> {error, {no_such_group, Name}};
        [{Name, Members}] -> Members
    end.

which_groups() ->
    [K || {K, _Members} <- ets:tab2list(?PIDS_TABLE)].

count_available(Name) ->
    case ets:lookup(?PIDS_TABLE, Name) of

        [] -> {error, {no_such_group, Name}};

        [{Name, Members}] -> 
            NumAvail = lists:foldl(
                fun(Member, Acc) -> 
                    case merle_client:get_checkout_state(Member) of
                        {false, _} ->
                            Acc + 1;
                        _ ->
                            Acc
                    end
                end, 
                0, 
                Members
            ),
            
            {length(Members), NumAvail}
    end.    
    
clean_locks() ->
    L = ets:tab2list(?PIDS_TABLE),

    {Cleaned, Connections} = lists:foldl(
        fun({_, Pids}, {C, V}) -> 
            {C2, V2} = clean_locks(Pids),
            {C + C2, V + V2}
        end, 
        {0, 0}, 
        L
    ),
    
    log4erl:error("Cleaned ~p merle locks on ~p valid connections", [Cleaned, Connections]),
    
    {Cleaned, Connections}.

clean_locks(Clients) ->
    NowSecs = now_secs(),
    CleanLocksIntervalSecs = ?CLEAN_LOCKS_INTERVAL div 1000,

    Acc = lists:foldl(
        fun(Client, {NumCleaned, ValidConnections}) ->
                        
            % clear locks with stale last_unlocked time 
            NumCleaned2 = case merle_client:get_checkout_state(Client) of
                {false, _} ->
                    NumCleaned;

                {true, indefinite} ->
                    NumCleaned;

                {true, CheckOutTime} ->
                    case (CheckOutTime + CleanLocksIntervalSecs) < NowSecs of
                        true ->
                            merle_client:checkin(Client),
                            NumCleaned + 1;
                        false ->
                            NumCleaned
                    end
            end,
            
            % maintain a count of the number of valid connections
            ValidConnections2 = case merle_client:get_socket(Client) of
                undefined -> ValidConnections;
                _ -> ValidConnections + 1
            end,           
            
            {NumCleaned2, ValidConnections2}
        end,

        {0, 0},

        Clients
    ),
    
    Acc.
    

shift_rr_index(Name, MembersLen) ->
    ets:update_counter(?INDICES_TABLE, {Name, rr_index}, {2, 1, MembersLen, 1}).
    

get_client(random, Name) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {error, {no_process, Name}};
        [{Name, Members}] ->
            %% TODO:  we can get more inteligent, check queue size, reductions, etc.
            %% http://lethain.com/entry/2009/sep/12/load-balancing-across-erlang-process-groups/
            {_, _, X} = erlang:now(),

            Pid = lists:nth((X rem length(Members)) +1, Members),

            Pid
    end;

get_client(round_robin, Name) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {error, {no_process, Name}};
        [{Name, Members}] ->           
         
            MembersLen = length(Members),
        
            % Get the round robin index        
            RRIndex = shift_rr_index(Name, MembersLen),

            lists:nth(RRIndex, Members)
    end.

%%
%%  SERVER FUNCTIONS
%%

init([]) ->
    process_flag(trap_exit, true),
    ets:new(?PIDS_TABLE, [set, public, named_table, {read_concurrency, true}]),
    ets:new(?INDICES_TABLE, [set, public, named_table, {write_concurrency, true}]),
    
    PLC = timer:apply_interval(?CLEAN_LOCKS_INTERVAL, merle_pool, clean_locks, []),

    State = #server_state {
        periodic_lock_clean = PLC
    },

    {ok, State}.

handle_call({create, Name}, _From, S) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            ets:insert(?INDICES_TABLE, {{Name, rr_index}, 1}),
            ets:insert(?PIDS_TABLE, {Name, []});
        _ ->
            ok
    end,
    {reply, ok, S};

handle_call({join, Name, Pid}, _From, S) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {reply, no_such_group, S};
        [{Name, Members}] ->

            % NOTE: skip one index since we are about to grow the list, this prevents collisions
            shift_rr_index(Name, length(Members)),

            % insert new pid into the table
            ets:insert(?PIDS_TABLE, {Name, [Pid | Members]}),

            % NOTE: link processes on join, so that if client dies, we remove it from the pool
            link(Pid),

            {reply, ok, S}
    end;
            
handle_call({leave, Name, Pid}, _From, S) ->
    case ets:lookup(?PIDS_TABLE, Name) of
        [] ->
            {reply, no_such_group, S};
        [{Name, Members}] ->
            case lists:delete(Pid, Members) of
                [] ->
                    ets:delete(?PIDS_TABLE, Name);
                NewMembers ->
                    ets:insert(?PIDS_TABLE, {Name, NewMembers})
            end,
            unlink(Pid),
            {reply, ok, S}
     end;

handle_call({delete, Name}, _From, S) ->
    ets:delete(?PIDS_TABLE, Name),
    {reply, ok, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'EXIT', Pid, _} , S) ->
    log4erl:error("Caught local_pg2 EXIT... leaving pg"),
    del_member(Pid),
    {noreply, S};
    
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #server_state{ periodic_lock_clean=PLC }) ->
    ets:delete(?PIDS_TABLE),
    ets:delete(?INDICES_TABLE),

    timer:cancel(PLC),
    
    %%do not unlink, if this fails, dangling processes should be killed
    ok.



%%
%%  HELPER FUNCTIONS
%%

now_secs() ->
    {NowMegaSecs, NowSecs, _} = erlang:now(),
    (1.0e+6 * NowMegaSecs) + NowSecs.

del_member(Pid) ->
    L = ets:tab2list(?PIDS_TABLE),
    lists:foreach(fun(Elem) -> del_member_func(Elem, Pid) end, L).
                   
del_member_func({Name, Members}, Pid) ->
    case lists:member(Pid, Members) of
          true ->
              case lists:delete(Pid, Members) of
                  [] ->
                      ets:delete(?PIDS_TABLE, Name);
                  NewMembers ->
                      ets:insert(?PIDS_TABLE, {Name, NewMembers})
              end;
          false ->
              ok
     end.