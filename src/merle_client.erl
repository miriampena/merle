-module(merle_client).

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2]).

-export([checkout/3, checkin/1, get_checkout_state/1, get_socket/1]).

-include_lib("canary/include/canary.hrl").

-define(RESTART_INTERVAL, 5000). %% retry each 5 seconds.

-record(state, {
    host,
    port,
    index,

    socket,                 % memcached connection socket
    socket_creator,

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
    gen_server:call(Pid, {checkin, dru:pytime()}).


get_checkout_state(Pid) ->
    gen_server:call(Pid, {get_checkout_state, dru:pytime()}).


get_socket(Pid) ->
    gen_server:call(Pid, {get_socket, dru:pytime()}).


%%
%%  SERVER CALL HANDLERS
%%


%%
%%  Handle checkout events.  Mark this server as used, and note the time.
%%  Bind a monitor with the checking out process.
%%
handle_call({checkout, _, CheckoutTime}, _From, State = #state{checked_out = true}) ->
    record_call_latency(<<"ClientCheckout">>, CheckoutTime),
    lager:error("Checkout: busy"),
    {reply, busy, State};
handle_call({checkout, _, CheckoutTime}, _From, State = #state{socket_creator = SocketCreator, socket = undefined}) ->
    % NOTE: initializes socket when none found
    record_call_latency(<<"ClientCheckout">>, CheckoutTime),
    lager:error("Checkout: no socket"),

    {
        reply, no_socket,
        case SocketCreator of
            undefined -> connect_socket(State);
            _ -> State
        end
    };
handle_call({checkout, BorrowerPid, CheckoutTime}, _From, State = #state{socket = Socket, monitor = PrevMonitor}) ->
    record_call_latency(<<"ClientCheckout">>, CheckoutTime),
    lager:error("Checkout: ok"),

    % handle any previously existing monitors
    case PrevMonitor of
        undefined ->
            ok;
        _ ->
            true = erlang:demonitor(PrevMonitor)
    end,

    Monitor = erlang:monitor(process, BorrowerPid),

    {reply, Socket, check_out_state(State#state{monitor = Monitor}, CheckoutTime)};

%%
%%  Handle checkin events.  Demonitor perviously monitored process, and mark as checked in
%%
handle_call({checkin, CallTime}, _From, State = #state{monitor = PrevMonitor}) ->
    record_call_latency(<<"ClientCheckin">>, CallTime),
    lager:error("Checkin"),

    case PrevMonitor of
        undefined -> ok;
        _ ->
            true = erlang:demonitor(PrevMonitor)
    end,

    {reply, ok, check_in_state(State#state{monitor = undefined})};


%%
%%  Returns checkout state for the client in question
%%
handle_call({get_checkout_state, CallTime}, _From, State = #state{checked_out = CheckedOut, check_out_time = CheckOutTime}) ->
    record_call_latency(<<"ClientGetCheckoutState">>, CallTime),

    {reply, {CheckedOut, CheckOutTime}, State};


%%
%%  Returns socket for the client in question
%%
handle_call({get_socket, CallTime}, _From, State = #state{socket = Socket}) ->
    record_call_latency(<<"ClientGetSocket">>, CallTime),

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
    lager:error("Connect"),

    MerleClientPid = self(),

    SocketCreatorPid = spawn_link(
        fun() ->
            case merle:connect(Host, Port) of
                {ok, Socket} ->
                    lager:error("Connect - socket creator - initialized"),
                    MerleClientPid ! {link_socket, Socket};

                ignore ->
                    lager:error("Connect - socket creator - ignore"),
                    erlang:send_after(?RESTART_INTERVAL, MerleClientPid, 'connect');

                {error, Reason} ->
                    lager:error("Connect - socket creator - error"),
                    error_logger:error_report([memcached_connection_error,
                        {reason, Reason},
                        {host, Host},
                        {port, Port},
                        {restarting_in, ?RESTART_INTERVAL}]
                    ),

                    timer:send_after(?RESTART_INTERVAL, MerleClientPid, 'connect')
            end
        end
    ),

    {noreply, State#state{socket_creator = SocketCreatorPid}};


handle_info(
    {link_socket, Socket},
    #state{
            checked_out = true,
            socket = undefined
    } = State)
    ->
    lager:error("Link socket"),

    State2 = case is_process_alive(Socket) of
        true ->
            lager:error("Link socket - socket is alive"),
            link(Socket),
            Socket ! ping,
            check_in_state(State#state{socket = Socket});
        false ->
            lager:error("Link socket - socket is dead"),
            connect_socket(State)
    end,

    {noreply, State2};


%%
%%  Handles down events from monitored process.  Need to kill socket if this happens.
%%
handle_info({'DOWN', MonitorRef, _, _, _}, #state{socket=Socket, monitor=MonitorRef} = S) ->
    lager:error("merle_client caught a DOWN event"),

    case Socket of
        undefined -> ok;
        _ ->
            unlink(Socket),
            exit(Socket, kill)
    end,

    erlang:demonitor(MonitorRef),

    {noreply, connect_socket(S#state{monitor = undefined}), ?RESTART_INTERVAL};


%%
%%  Handles exit events on the memcached socket.  If this occurs need to reconnect.
%%
handle_info({'EXIT', Socket, _}, S = #state{socket = Socket}) ->
    lager:error("Socket exited"),
    {noreply, connect_socket(S), ?RESTART_INTERVAL};

handle_info({'EXIT', SocketCreator, _}, S = #state{socket_creator = SocketCreator}) ->
    lager:error("Socket creator exited"),
    {noreply, connect_socket(S#state{socket_creator = undefined}), ?RESTART_INTERVAL};

handle_info({'EXIT', _, normal}, S) ->
    {noreply, S};

handle_info(Msg = {'EXIT', _, Reason}, S) ->
    lager:error("Unexplained EXIT ~p", [Msg]),
    {stop, Reason, S};

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

record_call_latency(OpName, CallTime) ->
    canary:notify_metric(
        {
            histogram,
            #canary_metric_name{
                category = <<"Merle">>,
                label = [OpName, <<"MsgQLen">>],
                units = <<"length">>
            },
            slide,
            60
        },
        begin {_, L} = process_info(self(), message_queue_len), L end
    ),

    canary:notify_metric(
        {
            histogram,
            #canary_metric_name{
                category = <<"Merle">>,
                label = [OpName, <<"Latency">>],
                units = <<"milliseconds">>
            },
            slide,
            60
        },
        trunc((dru:pytime() - CallTime) * 1000)
    ).


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
