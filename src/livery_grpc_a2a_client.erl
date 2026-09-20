-module(livery_grpc_a2a_client).
-moduledoc """
A2A client transport over gRPC.

Register this module with `barrel_a2a_client:connect/2` and the A2A
client speaks gRPC to any agent whose Agent Card offers a `GRPC`
interface. Everything above the wire, card fetching and caching,
signature verification, credentials, retries and `barrel_a2a_remote_task`,
is unchanged.

```erlang
{ok, Agent} = barrel_a2a_client:connect(<<"https://agent.example">>, #{
    transports => [{<<"GRPC">>, livery_grpc_a2a_client}],
    prefer     => [grpc, jsonrpc]
}),
{ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"hello">>),
ok = barrel_a2a_client:close(Agent).
```

A connection is a process. livery_grpc's h2 connection delivers a unary
call's events to whoever opened the connection, so the transport owns
that process and runs unary calls inside it; they are serialised. A
streaming call is opened with its own per-stream handler process, so
streams run concurrently with each other and with unary calls, and each
forwards `{a2a_stream, Ref, _}` to its owner.

Notes on the mapping:

- The interface URL gives the host, the port and the scheme (`https`
  means TLS with ALPN). Its path is ignored: gRPC routes on the fully
  qualified method name, not on a base path.
- `CallOpts` headers become call metadata unchanged, so `A2A-Version`,
  `A2A-Extensions` and the credentials the client built travel as the
  A2A specification requires.
- A `timeout` becomes `grpc-timeout`, so the server enforces the same
  deadline the caller set.
- An error comes back as a `barrel_a2a_error:error()` rebuilt from the
  `google.rpc.Status` in `grpc-status-details-bin`, so the caller sees
  the A2A error type, not just the gRPC status.
""".

-behaviour(barrel_a2a_client_transport).
-behaviour(gen_server).

-export([connect/2, call/4, stream/5, cancel_stream/2, close/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Run in the per-stream process.
-export([stream_init/1]).

-define(DEFAULT_TIMEOUT, 30000).
%% Slack over the call deadline so the server's own `deadline_exceeded`
%% reaches us before the local gen_server call gives up.
-define(CALL_SLACK, 2000).
%% A stream process is parked in a selective receive on its h2 events, so
%% a cancel cannot interrupt it. It polls instead: this is the longest a
%% cancel waits before the stream is reset.
-define(CANCEL_POLL_MS, 100).

-record(state, {
    conn :: livery_grpc_client:conn(),
    %% Ref => #{pid, owner}
    streams = #{} :: #{reference() => map()}
}).

-type state() :: #state{}.

%%====================================================================
%% barrel_a2a_client_transport
%%====================================================================

-doc "Open a connection to the agent's `GRPC` interface.".
-spec connect(barrel_a2a_agent_card:interface(), map()) ->
    {ok, pid()} | {error, barrel_a2a_error:error()}.
connect(#{<<"url">> := Url}, Opts) ->
    case target(Url) of
        {ok, Target} ->
            %% Unlinked: the connection outlives the process that opened
            %% it, exactly like the agent handle it belongs to.
            %% `close/1` is what ends it.
            case gen_server:start(?MODULE, {Target, Opts}, []) of
                {ok, Pid} -> {ok, Pid};
                {error, {shutdown, Reason}} -> {error, barrel_a2a_error:transport(Reason)};
                {error, Reason} -> {error, barrel_a2a_error:transport(Reason)}
            end;
        {error, Reason} ->
            {error, barrel_a2a_error:invalid(<<"url">>, format(Reason))}
    end;
connect(Interface, _Opts) ->
    {error, barrel_a2a_error:invalid(<<"url">>, format({no_url, Interface}))}.

-doc "Invoke a unary operation and return its reply object.".
-spec call(pid(), barrel_a2a:op(), barrel_a2a:object(), barrel_a2a_client_transport:call_opts()) ->
    {ok, barrel_a2a:json()} | {error, barrel_a2a_error:error()}.
call(Conn, Op, Request, CallOpts) ->
    Timeout = timeout(CallOpts),
    try
        gen_server:call(Conn, {call, Op, Request, CallOpts}, bounded(Timeout))
    catch
        exit:{timeout, _} -> {error, barrel_a2a_error:new(timeout)};
        exit:Reason -> {error, barrel_a2a_error:transport(Reason)}
    end.

-doc """
Open a server-streaming operation.

Events reach `Owner` as `{a2a_stream, Ref, {event, StreamResponse}}`,
followed by exactly one `{a2a_stream, Ref, done}` or
`{a2a_stream, Ref, {error, Error}}`.
""".
-spec stream(
    pid(),
    barrel_a2a:op(),
    barrel_a2a:object(),
    pid(),
    barrel_a2a_client_transport:call_opts()
) ->
    {ok, reference()} | {error, barrel_a2a_error:error()}.
stream(Conn, Op, Request, Owner, CallOpts) ->
    try
        gen_server:call(Conn, {stream, Op, Request, Owner, CallOpts}, bounded(?DEFAULT_TIMEOUT))
    catch
        exit:Reason -> {error, barrel_a2a_error:transport(Reason)}
    end.

-doc "Stop a stream early, resetting it so the agent sees a disconnect.".
-spec cancel_stream(pid(), reference()) -> ok.
cancel_stream(Conn, Ref) ->
    try
        gen_server:call(Conn, {cancel_stream, Ref}, bounded(?DEFAULT_TIMEOUT))
    catch
        exit:_Reason -> ok
    end.

-doc "Close the connection and reset any stream still running.".
-spec close(pid()) -> ok.
close(Conn) ->
    try
        gen_server:stop(Conn)
    catch
        exit:_Reason -> ok
    end.

%%====================================================================
%% gen_server
%%====================================================================

-spec init({map(), map()}) -> {ok, state()} | {stop, term()}.
init({#{host := Host, port := Port, transport := Transport, authority := Authority}, Opts}) ->
    ConnOpts = #{
        transport => Transport,
        authority => Authority,
        ssl_opts => ssl_opts(Opts)
    },
    case livery_grpc_client:connect(Host, Port, ConnOpts) of
        {ok, Conn} -> {ok, #state{conn = Conn}};
        {error, Reason} -> {stop, Reason}
    end.

-spec handle_call(term(), gen_server:from(), state()) -> {reply, term(), state()}.
handle_call({call, Op, Request, CallOpts}, _From, State) ->
    {reply, unary(State#state.conn, Op, Request, CallOpts), State};
handle_call({stream, Op, Request, Owner, CallOpts}, _From, State) ->
    open_stream(Op, Request, Owner, CallOpts, State);
handle_call({cancel_stream, Ref}, _From, State) ->
    {reply, ok, cancel(Ref, State)};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    {noreply, forget_stream_by_pid(Pid, State)};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, #state{conn = Conn}) ->
    %% Closing the connection resets every stream still open on it.
    livery_grpc_client:close(Conn).

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Unary
%%====================================================================

-spec unary(
    livery_grpc_client:conn(),
    barrel_a2a:op(),
    barrel_a2a:object(),
    barrel_a2a_client_transport:call_opts()
) ->
    {ok, barrel_a2a:json()} | {error, barrel_a2a_error:error()}.
unary(Conn, Op, Request, CallOpts) ->
    case method(Op) of
        {ok, #{input := Input, output := Output} = Method} ->
            try livery_grpc_a2a_codec:to_pb(Input, Request) of
                Message ->
                    Result = livery_grpc_client:call(Conn, Method, Message, call_opts(CallOpts)),
                    decode_reply(Output, Result)
            catch
                error:{a2a_codec, Reason} ->
                    {error, barrel_a2a_error:invalid(<<"request">>, format(Reason))}
            end;
        error ->
            {error, barrel_a2a_error:new(method_not_found, format({unknown_op, Op}))}
    end.

-spec decode_reply(atom(), livery_grpc_client:call_result()) ->
    {ok, barrel_a2a:json()} | {error, barrel_a2a_error:error()}.
decode_reply(Output, {ok, Message}) when is_map(Message) ->
    try
        {ok, livery_grpc_a2a_codec:to_json(Output, Message)}
    catch
        error:{a2a_codec, Reason} ->
            {error, barrel_a2a_error:new(invalid_agent_response, format(Reason))}
    end;
decode_reply(_Output, {error, {Status, Message}}) when is_atom(Status) ->
    {error, livery_grpc_a2a_codec:error_from_status(Status, Message, undefined)};
decode_reply(_Output, {error, {Status, Message, Details}}) when is_atom(Status) ->
    {error, livery_grpc_a2a_codec:error_from_status(Status, Message, Details)};
decode_reply(_Output, {error, Reason}) ->
    {error, barrel_a2a_error:transport(Reason)};
decode_reply(_Output, Other) ->
    {error, barrel_a2a_error:new(invalid_agent_response, format(Other))}.

%%====================================================================
%% Streaming
%%====================================================================

-spec open_stream(
    barrel_a2a:op(),
    barrel_a2a:object(),
    pid(),
    barrel_a2a_client_transport:call_opts(),
    state()
) ->
    {reply, {ok, reference()} | {error, barrel_a2a_error:error()}, state()}.
open_stream(Op, Request, Owner, CallOpts, #state{conn = Conn} = State) ->
    case method(Op) of
        {ok, #{input := Input} = Method} ->
            try livery_grpc_a2a_codec:to_pb(Input, Request) of
                Message ->
                    Ref = make_ref(),
                    Args = #{
                        conn => Conn,
                        method => Method,
                        message => Message,
                        owner => Owner,
                        ref => Ref,
                        opts => call_opts(CallOpts),
                        parent => self()
                    },
                    {Pid, MRef} = spawn_monitor(?MODULE, stream_init, [Args]),
                    await_open(Ref, Pid, MRef, Owner, State)
            catch
                error:{a2a_codec, Reason} ->
                    {reply, {error, barrel_a2a_error:invalid(<<"request">>, format(Reason))}, State}
            end;
        error ->
            Error = barrel_a2a_error:new(method_not_found, format({unknown_op, Op})),
            {reply, {error, Error}, State}
    end.

%% Wait for the stream to exist before answering. That way the caller
%% learns of an open failure as an error rather than as a stream that
%% dies immediately, and a `cancel_stream/2` right after `stream/5`
%% always finds a stream id to reset.
-spec await_open(reference(), pid(), reference(), pid(), state()) ->
    {reply, {ok, reference()} | {error, barrel_a2a_error:error()}, state()}.
await_open(Ref, Pid, MRef, Owner, #state{streams = Streams} = State) ->
    receive
        {stream_open, Ref} ->
            Stream = #{pid => Pid, owner => Owner},
            {reply, {ok, Ref}, State#state{streams = Streams#{Ref => Stream}}};
        {stream_failed, Ref, Error} ->
            {reply, {error, Error}, State};
        {'DOWN', MRef, process, Pid, Reason} ->
            {reply, {error, barrel_a2a_error:transport(Reason)}, State}
    after ?DEFAULT_TIMEOUT ->
        exit(Pid, kill),
        {reply, {error, barrel_a2a_error:new(timeout, <<"stream did not open">>)}, State}
    end.

%% The stream process owns its h2 stream, so it does the resetting; the
%% cancel only has to reach it. That keeps it the only sender of
%% `{a2a_stream, Ref, _}`, which is what orders the events ahead of the
%% terminal message.
-spec cancel(reference(), state()) -> state().
cancel(Ref, #state{streams = Streams} = State) ->
    case maps:find(Ref, Streams) of
        {ok, #{pid := Pid}} ->
            Pid ! {a2a_cancel, Ref},
            State;
        error ->
            State
    end.

-spec forget_stream_by_pid(pid(), state()) -> state().
forget_stream_by_pid(Pid, #state{streams = Streams} = State) ->
    Live = maps:filter(fun(_Ref, #{pid := P}) -> P =/= Pid end, Streams),
    State#state{streams = Live}.

%%--------------------------------------------------------------------
%% The per-stream process
%%--------------------------------------------------------------------

-doc false.
-spec stream_init(map()) -> ok.
stream_init(#{conn := Conn, method := Method, message := Message} = Args) ->
    #{owner := Owner, ref := Ref, opts := Opts, parent := Parent} = Args,
    %% `open/3` routes this stream's h2 events here, so the loop below
    %% runs without touching the connection owner.
    case livery_grpc_client:open(Conn, Method, Opts) of
        {ok, Call} ->
            Parent ! {stream_open, Ref},
            send_request(Call, Message, Owner, Ref, Opts);
        {error, Reason} ->
            %% Nothing was opened, so the failure is the answer to
            %% `stream/5` rather than a terminal stream message.
            Parent ! {stream_failed, Ref, barrel_a2a_error:transport(Reason)},
            ok
    end.

-spec send_request(map(), map(), pid(), reference(), map()) -> ok.
send_request(Call, Message, Owner, Ref, Opts) ->
    case livery_grpc_client:send(Call, Message) of
        ok ->
            case livery_grpc_client:send_end(Call) of
                ok -> recv_loop(Call, Owner, Ref, recv_timeout(Opts));
                {error, Reason} -> terminal(Owner, Ref, transport_error(Reason))
            end;
        {error, Reason} ->
            terminal(Owner, Ref, transport_error(Reason))
    end.

%% `Timeout` bounds one event, not the whole stream, so it restarts on
%% every message received. The wait is sliced so `{a2a_cancel, Ref}` is
%% noticed: `recv/2` blocks in a selective receive that would otherwise
%% ignore it until the whole timeout elapsed.
-spec recv_loop(map(), pid(), reference(), timeout()) -> ok.
recv_loop(Call, Owner, Ref, Timeout) ->
    poll(Call, Owner, Ref, Timeout, until(Timeout)).

-spec poll(map(), pid(), reference(), timeout(), integer() | infinity) -> ok.
poll(Call, Owner, Ref, Timeout, Until) ->
    case cancel_requested(Ref) of
        true ->
            _ = livery_grpc_client:cancel(Call),
            terminal(Owner, Ref, done);
        false ->
            receive_one(Call, Owner, Ref, Timeout, Until)
    end.

-spec receive_one(map(), pid(), reference(), timeout(), integer() | infinity) -> ok.
receive_one(Call, Owner, Ref, Timeout, Until) ->
    case livery_grpc_client:recv(Call, slice(Until)) of
        {ok, Message, Call1} ->
            Event = livery_grpc_a2a_codec:to_json('StreamResponse', Message),
            Owner ! {a2a_stream, Ref, {event, Event}},
            poll(Call1, Owner, Ref, Timeout, until(Timeout));
        {eof, ok, _Call1} ->
            terminal(Owner, Ref, done);
        {eof, {Status, Message}, _Call1} ->
            terminal(Owner, Ref, status_error(Status, Message, undefined));
        {eof, {Status, Message, Details}, _Call1} ->
            terminal(Owner, Ref, status_error(Status, Message, Details));
        {error, timeout, Call1} ->
            case expired(Until) of
                true -> terminal(Owner, Ref, {error, barrel_a2a_error:new(timeout)});
                false -> poll(Call1, Owner, Ref, Timeout, Until)
            end;
        {error, {stream_reset, _Reason}, _Call1} ->
            %% The peer reset the stream; a cancel of ours never gets
            %% here because it is handled before the next receive.
            terminal(Owner, Ref, transport_error(stream_reset));
        {error, Reason, _Call1} ->
            terminal(Owner, Ref, transport_error(Reason))
    end.

-spec cancel_requested(reference()) -> boolean().
cancel_requested(Ref) ->
    receive
        {a2a_cancel, Ref} -> true
    after 0 -> false
    end.

-spec until(timeout()) -> integer() | infinity.
until(infinity) -> infinity;
until(Ms) -> erlang:monotonic_time(millisecond) + Ms.

-spec slice(integer() | infinity) -> non_neg_integer().
slice(infinity) -> ?CANCEL_POLL_MS;
slice(Until) -> min(?CANCEL_POLL_MS, max(0, Until - erlang:monotonic_time(millisecond))).

-spec expired(integer() | infinity) -> boolean().
expired(infinity) -> false;
expired(Until) -> erlang:monotonic_time(millisecond) >= Until.

-spec status_error(atom(), binary(), binary() | undefined) ->
    {error, barrel_a2a_error:error()}.
status_error(Status, Message, Details) ->
    {error, livery_grpc_a2a_codec:error_from_status(Status, Message, Details)}.

-spec transport_error(term()) -> {error, barrel_a2a_error:error()}.
transport_error(Reason) ->
    {error, barrel_a2a_error:transport(Reason)}.

-spec terminal(pid(), reference(), done | {error, barrel_a2a_error:error()}) -> ok.
terminal(Owner, Ref, done) ->
    Owner ! {a2a_stream, Ref, done},
    ok;
terminal(Owner, Ref, {error, Error}) ->
    Owner ! {a2a_stream, Ref, {error, Error}},
    ok.

%%====================================================================
%% Options and targets
%%====================================================================

%% The eleven operations, by the rpc that carries each one.
-spec method(barrel_a2a:op()) -> {ok, livery_grpc_service:method()} | error.
method(Op) ->
    case rpc_name(Op) of
        undefined -> error;
        Name -> livery_grpc_service:method(livery_grpc_a2a_codec:proto(), 'A2AService', Name)
    end.

-spec rpc_name(barrel_a2a:op()) -> atom() | undefined.
rpc_name(send_message) -> 'SendMessage';
rpc_name(send_streaming_message) -> 'SendStreamingMessage';
rpc_name(get_task) -> 'GetTask';
rpc_name(list_tasks) -> 'ListTasks';
rpc_name(cancel_task) -> 'CancelTask';
rpc_name(subscribe_to_task) -> 'SubscribeToTask';
rpc_name(create_push_config) -> 'CreateTaskPushNotificationConfig';
rpc_name(get_push_config) -> 'GetTaskPushNotificationConfig';
rpc_name(list_push_configs) -> 'ListTaskPushNotificationConfigs';
rpc_name(delete_push_config) -> 'DeleteTaskPushNotificationConfig';
rpc_name(get_extended_agent_card) -> 'GetExtendedAgentCard';
rpc_name(_Op) -> undefined.

%% The client's headers become call metadata, and its timeout becomes
%% the call deadline so the agent enforces it too.
-spec call_opts(barrel_a2a_client_transport:call_opts()) -> livery_grpc_client:call_opts().
call_opts(CallOpts) ->
    Metadata = [{lower(Name), Value} || {Name, Value} <- maps:get(headers, CallOpts, [])],
    Base = #{metadata => Metadata},
    case timeout(CallOpts) of
        infinity -> Base;
        Ms -> Base#{deadline => Ms, timeout => Ms + ?CALL_SLACK}
    end.

-spec timeout(barrel_a2a_client_transport:call_opts()) -> timeout().
timeout(CallOpts) ->
    case maps:get(timeout, CallOpts, ?DEFAULT_TIMEOUT) of
        infinity -> infinity;
        Ms when is_integer(Ms), Ms > 0 -> Ms;
        _Other -> ?DEFAULT_TIMEOUT
    end.

%% A stream has no overall deadline of its own: the bound is how long a
%% single event may take to arrive.
-spec recv_timeout(livery_grpc_client:call_opts()) -> timeout().
recv_timeout(Opts) ->
    maps:get(timeout, Opts, ?DEFAULT_TIMEOUT).

-spec bounded(timeout()) -> timeout().
bounded(infinity) -> infinity;
bounded(Ms) -> Ms + ?CALL_SLACK.

-spec ssl_opts(map()) -> [ssl:tls_client_option()].
ssl_opts(Opts) ->
    case maps:get(ssl_options, Opts, maps:get(ssl_opts, Opts, undefined)) of
        undefined -> [];
        SslOpts -> SslOpts
    end.

%% gRPC routes on the fully qualified method name, so only the scheme,
%% the host and the port of the interface URL are used.
-spec target(binary()) -> {ok, map()} | {error, term()}.
target(Url) when is_binary(Url) ->
    case uri_string:parse(Url) of
        #{scheme := Scheme, host := Host} = Parsed when Host =/= <<>> ->
            case transport(Scheme) of
                {ok, Transport, DefaultPort} ->
                    Port = maps:get(port, Parsed, DefaultPort),
                    {ok, #{
                        host => binary_to_list(Host),
                        port => Port,
                        transport => Transport,
                        authority => authority(Host, Port, DefaultPort)
                    }};
                error ->
                    {error, {unsupported_scheme, Scheme}}
            end;
        _Other ->
            {error, {invalid_url, Url}}
    end;
target(Url) ->
    {error, {invalid_url, Url}}.

-spec transport(binary()) -> {ok, tcp | ssl, inet:port_number()} | error.
transport(<<"https">>) -> {ok, ssl, 443};
transport(<<"http">>) -> {ok, tcp, 80};
transport(<<"grpcs">>) -> {ok, ssl, 443};
transport(<<"grpc">>) -> {ok, tcp, 80};
transport(_Scheme) -> error.

-spec authority(binary(), inet:port_number(), inet:port_number()) -> binary().
authority(Host, Port, Port) -> Host;
authority(Host, Port, _Default) -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>.

-spec lower(binary()) -> binary().
lower(Name) -> string:lowercase(Name).

-spec format(term()) -> binary().
format(Term) -> iolist_to_binary(io_lib:format("~0p", [Term])).
