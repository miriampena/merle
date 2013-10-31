-module(merle_client).

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-export([checkout/3, checkin/1, get_checkout_state/1, get_socket/1]).

-define(RESTART_INTERVAL, 1000). %% retry each 1 second.

-record(state, {
    host,
    port,
    index,

    socket,                 % memcached connection socket
    socket_creator,

    socket_creator_recon_tref,  % tref which sends a reconnect message in the event some
                                % socket creation pid exits unexpectedly

    monitor,                % represents a monitor bw checking out process and me

    checked_out,            % boolean indicating whether this connection is checked out or not
    check_out_time          % timestamp marking when this connection was checked out
}).


start_link([Host, Port, Index]) ->
    gen_server:start_link(?MODULE, [Host, Port, Index], []).


init([Host, Port, Index]) ->
    lager:info("Merle client ~p is STARTING", [[Host, Port, Index]]),

    erlang:process_flag(trap_exit, true),

    merle_pool:create({Host, Port}),
    merle_pool:join({Host, Port}, Index, self()),

    {
        ok,
        check_in_state(
            #state{
                host = Host,
                port = Port,
                index = Index
            }
        )
    }.


%%
%%  API
%%


checkout(Pid, BorrowerPid, CheckoutTime) ->
    gen_server:call(Pid, {checkout, BorrowerPid, CheckoutTime}).


checkin(Pid) ->
    gen_server:call(Pid, {checkin}).


get_checkout_state(Pid) ->
    gen_server:call(Pid, {get_checkout_state}).


get_socket(Pid) ->
    gen_server:call(Pid, {get_socket}).


%%
%%  SERVER CALL HANDLERS
%%


%%
%%  Handle checkout events.  Mark this server as used, and note the time.
%%  Bind a monitor with the checking out process.
%%
handle_call({checkout, _, _CheckoutTime}, _From, State = #state{checked_out = true}) ->
    lager:info("Checkout: busy"),
    {reply, busy, State};
handle_call({checkout, _, _CheckoutTime}, _From, State = #state{socket_creator = SocketCreator, socket = undefined}) ->
    % NOTE: initializes socket when none found
    lager:info("Checkout: no socket"),

    {
        reply, no_socket,
        case SocketCreator of
            undefined -> connect_socket(State);
            _ -> State
        end
    };
handle_call({checkout, BorrowerPid, CheckoutTime}, _From, State = #state{socket = Socket, monitor = PrevMonitor}) ->
    lager:info("Checkout: ok"),

    % NOTE: handle race condition of a dead socket somehow still being referenced
    case is_process_alive(Socket) of
        true ->
            % handle any previously existing monitors
            case PrevMonitor of
                undefined ->
                    ok;
                _ ->
                    true = erlang:demonitor(PrevMonitor)
            end,

            Monitor = erlang:monitor(process, BorrowerPid),

            {reply, Socket, check_out_state(State#state{monitor = Monitor}, CheckoutTime)};

        false ->
            {reply, no_socket, connect_socket(State)}
    end;


%%
%%  Handle checkin events.  Demonitor perviously monitored process, and mark as checked in
%%
handle_call({checkin}, _From, State = #state{monitor = PrevMonitor}) ->
    lager:info("Checkin"),

    case PrevMonitor of
        undefined -> ok;
        _ ->
            true = erlang:demonitor(PrevMonitor)
    end,

    {reply, ok, check_in_state(State#state{monitor = undefined})};


%%
%%  Returns checkout state for the client in question
%%
handle_call({get_checkout_state}, _From, State = #state{checked_out = CheckedOut, check_out_time = CheckOutTime}) ->
    {reply, {CheckedOut, CheckOutTime}, State};


%%
%%  Returns socket for the client in question
%%
handle_call({get_socket}, _From, State = #state{socket = Socket}) ->
    {reply, Socket, State};


handle_call(_Call, _From, S) ->
    {reply, ok, S}.


%%
%%  Handles 'connect' messages -> initializes socket on host/port, saving a reference
%%
handle_info(
    'connect',
    #state{
            host = Host,
            port = Port,
            checked_out = true,
            socket_creator = undefined,
            socket = undefined
    } = State)
    ->
    lager:info("Connect"),

    MerleClientPid = self(),

    SocketCreatorPid = spawn_link(
        fun() ->
            case merle:connect(Host, Port) of
                {ok, Socket} ->
                    lager:info("Connect - socket creator - initialized"),
                    MerleClientPid ! {link_socket, Socket};

                ignore ->
                    lager:info("Connect - socket creator - ignore"),
                    erlang:send_after(?RESTART_INTERVAL, MerleClientPid, 'connect');

                {error, Reason} ->
                    lager:info("Connect - socket creator - error"),
                    error_logger:error_report([memcached_connection_error,
                        {reason, Reason},
                        {host, Host},
                        {port, Port},
                        {restarting_in, ?RESTART_INTERVAL}]
                    ),
                    erlang:send_after(?RESTART_INTERVAL, MerleClientPid, 'connect')

            end
        end
    ),

    {noreply, State#state{socket_creator = SocketCreatorPid}};


handle_info(
    {link_socket, Socket},
    #state{
            checked_out = true,
            socket = undefined,
            socket_creator_recon_tref = TRef
    } = State)
    ->
    lager:info("Link socket"),

    State2 = case is_process_alive(Socket) of
        true ->
            lager:info("Link socket - socket is alive"),
            link(Socket),
            Socket ! ping,
            check_in_state(State#state{socket = Socket});
        false ->
            lager:info("Link socket - socket is dead"),
            connect_socket(State)
    end,

    case TRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(TRef)
    end,

    {noreply, State2#state{socket_creator_recon_tref = undefined}};


%%
%%  Handles down events from monitored process.  Need to kill socket if this happens.
%%
handle_info({'DOWN', MonitorRef, _, _, _}, #state{socket=Socket, monitor=MonitorRef} = S) ->
    lager:info("merle_client caught a DOWN event"),

    case Socket of
        undefined -> ok;
        _ ->
            unlink(Socket),
            exit(Socket, kill)
    end,

    erlang:demonitor(MonitorRef),

    {noreply, connect_socket(S#state{monitor = undefined})};


%%
%%  Handles exit events on the memcached socket.  If this occurs need to reconnect.
%%
handle_info({'EXIT', Socket, _}, S = #state{socket = Socket}) ->
    lager:info("Socket exited"),
    {noreply, connect_socket(S)};

handle_info({'EXIT', SocketCreator, normal}, S = #state{socket_creator = SocketCreator}) ->
    lager:info("Socket creator exited normally"),
    {noreply, S#state{socket_creator = undefined}};

handle_info({'EXIT', SocketCreator, _}, S = #state{socket_creator = SocketCreator}) ->
    lager:error("Socket creator exited abnormally"),

    % in the abnormal case, we'll want to spawn a reconnect message
    TRef = erlang:send_after(?RESTART_INTERVAL, self(), 'connect'),

    {noreply, S#state{socket_creator_recon_tref = TRef, socket_creator = undefined}};

handle_info({'EXIT', _, _}, S) ->
    {noreply, S};

handle_info(_Info, S) ->
    {noreply, S}.
    

handle_cast(_Cast, S) ->
    {noreply, S}.
    

terminate(_Reason, #state{socket = undefined}) ->
    lager:error("Merle watcher TERMINATING, socket is empty!"),
    ok;

terminate(_Reason, #state{socket = Socket}) ->
    lager:error("Merle watcher TERMINATING, killing socket!"),
    erlang:exit(Socket, watcher_died),
    ok.

%%
%%  HELPER FUNCTIONS
%%

connect_socket(State = #state{}) ->
    self() ! 'connect',
    check_out_state_indefinitely(State#state{socket = undefined}).


check_out_state_indefinitely(State = #state{}) ->
    check_out_state(State, indefinite).


check_out_state(State = #state{}, CheckOutTime) ->
    State#state{
        checked_out = true,
        check_out_time = CheckOutTime
    }.


check_in_state(State = #state{}) ->
    State#state{
        checked_out = false,
        check_out_time = undefined
    }.
