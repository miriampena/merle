-module(merle_watcher).

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-export([merle_connection/1, monitor/2, demonitor/1]).

-define(RESTART_INTERVAL, 5000). %% retry each 5 seconds. 

-record(state, {
    mcd_pid, 
    host,
    port,
    monitor
}).

start_link([Host, Port]) ->
    gen_server:start_link(?MODULE, [Host, Port], []).

init([Host, Port]) ->
   log4erl:info("Merle watcher initialized!"),
   erlang:process_flag(trap_exit, true),

   SelfPid = self(),

   local_pg2:create({Host, Port}),
   local_pg2:join({Host, Port}, SelfPid),

   local_pg2:checkout_pid(SelfPid),

   SelfPid ! connect,   
   
   {ok, #state{mcd_pid = undefined, host = Host, port = Port}}.

merle_connection(Pid) ->
    gen_server:call(Pid, mcd_pid).

monitor(Pid, OwnerPid) ->
    gen_server:call(Pid, {mark_owner, OwnerPid}).

demonitor(Pid) ->
    gen_server:call(Pid, demonitor).

handle_call(mcd_pid, _From, State = #state{mcd_pid = McdPid}) ->
   {reply, McdPid, State};

handle_call({monitor, MonitorPid}, _From, State = #state{monitor = PrevMonitor}) ->
   case PrevMonitor of
       undefined -> ok;
       _ ->
           true = erlang:demonitor(PrevMonitor)
   end,
    
   Monitor = erlang:monitor(process, MonitorPid),

   {reply, ok, State#state{monitor = Monitor}};

handle_call(demonitor, _From, State = #state{monitor = PrevMonitor}) ->
    true = erlang:demonitor(PrevMonitor),
    {reply, ok, State#state{monitor = undefined}};

handle_call(_Call, _From, S) ->
    {reply, ok, S}.
    
handle_info('connect', #state{mcd_pid = undefined, host = Host, port = Port} = State) ->
    case merle:connect(Host, Port) of
        {ok, Pid} ->

            local_pg2:checkin_pid(self()),

            {noreply, State#state{mcd_pid = Pid}};

        {error, Reason} ->
    	    receive 
    		    {'EXIT', _ , _} -> ok
    		after 2000 ->
    			ok
    	    end,

	        error_logger:error_report([memcached_not_started, 
	            {reason, Reason},
	            {host, Host},
	            {port, Port},
	            {restarting_in, ?RESTART_INTERVAL}]
	        ),
	        
            {noreply, State, ?RESTART_INTERVAL}
   end;

handle_info({'DOWN', MonitorRef, _, _, _}, #state{monitor=MonitorRef} = S) ->
    log4erl:info("merle_watcher caught a DOWN event"),
    
    local_pg2:checkin_pid(self()),
    
    true = erlang:demonitor(MonitorRef),

    {noreply, S#state{monitor = undefined}};
	
handle_info({'EXIT', Pid, _}, #state{mcd_pid = Pid} = S) ->
    local_pg2:checkout_pid(self()),
    
    self() ! connect,
    
    {noreply, S#state{mcd_pid = undefined}, ?RESTART_INTERVAL};
    
handle_info(_Info, S) ->
    error_logger:warning_report([{merle_watcher, self()}, {unknown_info, _Info}]),

    {noreply, S}.
    
handle_cast(_Cast, S) ->
    {noreply, S}.
    
terminate(_Reason, #state{mcd_pid = undefined}) ->
    log4erl:error("Merle watcher terminated, mcd pid is empty!"),
    ok;

terminate(_Reason, #state{mcd_pid = McdPid}) ->
    log4erl:error("Merle watcher terminated, killing mcd pid!"),
    erlang:exit(McdPid, watcher_died),
    ok.