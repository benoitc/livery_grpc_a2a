-module(livery_grpc_a2a).
-moduledoc """
The A2A gRPC binding: serve `lf.a2a.v1.A2AService` from a barrel_a2a
server.

[A2A](https://a2a-protocol.org) specification section 10 defines a gRPC
binding for the protocol. `service/1` turns a running barrel_a2a server
into a service registration for `livery_grpc:start_server/1`, so one
agent answers over gRPC exactly as it answers over JSON-RPC and
HTTP+JSON.

```erlang
{ok, Agent} = barrel_a2a_server:start(Card, #{handler => my_agent, listen => false}),
{ok, Server} = livery_grpc:start_server(#{
    port     => 50051,
    services => [livery_grpc_a2a:service(Agent)]
}).
```

Every RPC goes through `barrel_a2a_server_core:call/4`, the same
binding-neutral entry point the HTTP bindings use, so authentication,
version and extension negotiation, validation and task management behave
identically on every wire. This module only translates:

- the request message becomes an A2A JSON object
  (`livery_grpc_a2a_codec`),
- the `a2a-version`, `a2a-extensions` and `authorization` metadata
  become the request context,
- the reply becomes the response message,
- an error becomes a gRPC status with a `google.rpc.Status` in
  `grpc-status-details-bin`, carrying the A2A reason so the peer
  recovers the exact error type.

`SendStreamingMessage` and `SubscribeToTask` are server-streaming: the
request process subscribes to the task and forwards each event until the
task reaches a final state, the client disconnects, or the call deadline
passes.

See the [A2A guide](guides/a2a.md) for the Agent Card interface entry and
for sharing one agent between an HTTP listener and this one.
""".

-export([service/1, service/2]).

%% One callback per rpc, named after it in snake_case.
-export([
    send_message/2,
    send_streaming_message/3,
    get_task/2,
    list_tasks/2,
    cancel_task/2,
    subscribe_to_task/3,
    create_task_push_notification_config/2,
    get_task_push_notification_config/2,
    list_task_push_notification_configs/2,
    delete_task_push_notification_config/2,
    get_extended_agent_card/2
]).

-export_type([opts/0]).

-type opts() :: #{
    %% Reserved for binding-level options; none are defined yet.
    _ => _
}.

-define(SERVICE, 'A2AService').

%%====================================================================
%% Registration
%%====================================================================

-doc """
The service registration for `livery_grpc:start_server/1`.

`Server` is a barrel_a2a server pid (`barrel_a2a_server:start/2`). It
travels in the registration's `config`, so several A2A servers can be
served by separate gRPC listeners in one node.
""".
-spec service(pid()) -> livery_grpc:service_spec().
service(Server) ->
    service(Server, #{}).

-doc "`service/1` with binding options.".
-spec service(pid(), opts()) -> livery_grpc:service_spec().
service(Server, Opts) when is_pid(Server), is_map(Opts) ->
    #{
        proto => livery_grpc_a2a_codec:proto(),
        service => ?SERVICE,
        handler => ?MODULE,
        config => Opts#{server => Server}
    }.

%%====================================================================
%% Unary rpcs
%%====================================================================

-doc "SendMessage: send a message and wait for the outcome.".
-spec send_message(map(), livery_grpc_server:ctx()) -> livery_grpc_server:callback_result().
send_message(Request, Ctx) -> unary(send_message, Request, Ctx).

-doc "GetTask: the latest state of a task.".
-spec get_task(map(), livery_grpc_server:ctx()) -> livery_grpc_server:callback_result().
get_task(Request, Ctx) -> unary(get_task, Request, Ctx).

-doc "ListTasks: tasks matching a filter.".
-spec list_tasks(map(), livery_grpc_server:ctx()) -> livery_grpc_server:callback_result().
list_tasks(Request, Ctx) -> unary(list_tasks, Request, Ctx).

-doc "CancelTask: cancel a task in progress.".
-spec cancel_task(map(), livery_grpc_server:ctx()) -> livery_grpc_server:callback_result().
cancel_task(Request, Ctx) -> unary(cancel_task, Request, Ctx).

-doc "CreateTaskPushNotificationConfig.".
-spec create_task_push_notification_config(map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
create_task_push_notification_config(Request, Ctx) -> unary(create_push_config, Request, Ctx).

-doc "GetTaskPushNotificationConfig.".
-spec get_task_push_notification_config(map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
get_task_push_notification_config(Request, Ctx) -> unary(get_push_config, Request, Ctx).

-doc "ListTaskPushNotificationConfigs.".
-spec list_task_push_notification_configs(map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
list_task_push_notification_configs(Request, Ctx) -> unary(list_push_configs, Request, Ctx).

-doc "DeleteTaskPushNotificationConfig: replies with google.protobuf.Empty.".
-spec delete_task_push_notification_config(map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
delete_task_push_notification_config(Request, Ctx) -> unary(delete_push_config, Request, Ctx).

-doc "GetExtendedAgentCard: the authenticated card.".
-spec get_extended_agent_card(map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
get_extended_agent_card(Request, Ctx) -> unary(get_extended_agent_card, Request, Ctx).

-spec unary(barrel_a2a:op(), map(), livery_grpc_server:ctx()) ->
    livery_grpc_server:callback_result().
unary(Op, Request, #{method := #{input := Input, output := Output}} = Ctx) ->
    with_codec(fun() ->
        Json = livery_grpc_a2a_codec:to_json(Input, Request),
        case barrel_a2a_server_core:call(server(Ctx), Op, Json, req_ctx(Ctx)) of
            {ok, Reply} ->
                {ok, livery_grpc_a2a_codec:to_pb(Output, Reply)};
            {error, Error} ->
                {error, livery_grpc_a2a_codec:error_status(Error)};
            {stream, _Subscribe} ->
                %% The engine only streams for the two streaming rpcs.
                {error, {internal, <<"streaming reply on a unary method">>}}
        end
    end).

%%====================================================================
%% Server-streaming rpcs
%%====================================================================

-doc "SendStreamingMessage: send a message and stream the task's events.".
-spec send_streaming_message(map(), fun((map()) -> term()), livery_grpc_server:ctx()) ->
    ok | {error, term()}.
send_streaming_message(Request, Send, Ctx) ->
    stream(send_streaming_message, Request, Send, Ctx).

-doc "SubscribeToTask: stream the events of a task already running.".
-spec subscribe_to_task(map(), fun((map()) -> term()), livery_grpc_server:ctx()) ->
    ok | {error, term()}.
subscribe_to_task(Request, Send, Ctx) ->
    stream(subscribe_to_task, Request, Send, Ctx).

-spec stream(barrel_a2a:op(), map(), fun((map()) -> term()), livery_grpc_server:ctx()) ->
    ok | {error, term()}.
stream(Op, Request, Send, #{method := #{input := Input, output := Output}} = Ctx) ->
    Emit = fun(Event) -> Send(livery_grpc_a2a_codec:to_pb(Output, Event)) end,
    Deadline = deadline_at(maps:get(deadline, Ctx, infinity)),
    with_codec(fun() ->
        Json = livery_grpc_a2a_codec:to_json(Input, Request),
        case barrel_a2a_server_core:call(server(Ctx), Op, Json, req_ctx(Ctx)) of
            {stream, Subscribe} ->
                subscribe(Subscribe, Emit, Deadline);
            {ok, Reply} ->
                %% A message the engine could answer without starting a
                %% stream (a deduplicated resend). Its reply object is
                %% shaped like a StreamResponse already.
                _ = Emit(Reply),
                ok;
            {error, Error} ->
                {error, livery_grpc_a2a_codec:error_status(Error)}
        end
    end).

-spec subscribe(barrel_a2a_server_core:subscribe(), fun((map()) -> term()), integer() | infinity) ->
    ok | {error, term()}.
subscribe(Subscribe, Emit, Deadline) ->
    case Subscribe(self()) of
        {ok, Initial} -> emit_initial(Initial, Emit, Deadline);
        {error, Error} -> stream_error(Error)
    end.

%% The subscription hands back the events to write before the loop
%% starts (a task snapshot, usually). A final one ends the call there.
-spec emit_initial([map()], fun((map()) -> term()), integer() | infinity) -> ok | {error, term()}.
emit_initial([], Emit, Deadline) ->
    stream_loop(Emit, Deadline);
emit_initial([Event | Rest], Emit, Deadline) ->
    _ = Emit(Event),
    case barrel_a2a_event:is_final(Event) of
        true -> ok;
        false -> emit_initial(Rest, Emit, Deadline)
    end.

-spec stream_loop(fun((map()) -> term()), integer() | infinity) -> ok | {error, term()}.
stream_loop(Emit, Deadline) ->
    receive
        {a2a_task_event, _TaskId, Event} ->
            _ = Emit(Event),
            case barrel_a2a_event:is_final(Event) of
                true -> ok;
                false -> stream_loop(Emit, Deadline)
            end;
        {a2a_task_error, _TaskId, Error} ->
            stream_error(Error);
        {livery_disconnect, _Ref, _Reason} ->
            %% The peer is gone; nothing more can be written.
            ok;
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            ok;
        _Other ->
            stream_loop(Emit, Deadline)
    after remaining(Deadline) ->
        {error, {deadline_exceeded, <<"deadline exceeded">>}}
    end.

%% An error mid-stream can only travel in the trailers: some messages
%% have already been written, so there is no Trailers-Only reply left.
-spec stream_error(barrel_a2a_error:error()) -> {error, term()}.
stream_error(Error) ->
    {error, livery_grpc_a2a_codec:error_status(Error)}.

%%====================================================================
%% Request context
%%====================================================================

-spec server(livery_grpc_server:ctx()) -> pid().
server(#{config := #{server := Server}}) ->
    Server.

%% What this binding knows about the caller. `headers` are the call
%% metadata, so barrel_a2a's own auth hook sees `authorization` exactly
%% as it does over HTTP.
-spec req_ctx(livery_grpc_server:ctx()) -> barrel_a2a_server_core:req_ctx().
req_ctx(#{metadata := Metadata, req := Req}) ->
    Base = #{
        binding => grpc,
        headers => Metadata,
        version => metadata(<<"a2a-version">>, Metadata),
        extensions => barrel_a2a_extensions:parse_header(
            metadata(<<"a2a-extensions">>, Metadata)
        ),
        %% gRPC has no path tenant: the engine reads it from the
        %% request message's own `tenant` field.
        tenant => undefined,
        peer => livery_req:peer(Req)
    },
    case livery_ext:user(Req, undefined) of
        undefined -> Base;
        Principal -> Base#{principal => Principal}
    end.

-spec metadata(binary(), [{binary(), binary()}]) -> binary() | undefined.
metadata(Name, Metadata) ->
    case lists:keyfind(Name, 1, Metadata) of
        {_Name, Value} -> Value;
        false -> undefined
    end.

%%====================================================================
%% Deadlines and codec failures
%%====================================================================

%% `grpc-timeout` gives a duration; hold the instant it expires so a
%% long stream cannot restart the clock on every event.
-spec deadline_at(timeout()) -> integer() | infinity.
deadline_at(infinity) -> infinity;
deadline_at(Ms) -> erlang:monotonic_time(millisecond) + Ms.

-spec remaining(integer() | infinity) -> timeout().
remaining(infinity) -> infinity;
remaining(At) -> max(0, At - erlang:monotonic_time(millisecond)).

%% A conversion failure is a bug in this binding or a message the engine
%% built wrong, not something the caller can fix, so it is `internal`.
-spec with_codec(fun(() -> Result)) -> Result | {error, {internal, binary()}}.
with_codec(Run) ->
    try
        Run()
    catch
        error:{a2a_codec, Reason} ->
            {error, {internal, iolist_to_binary(io_lib:format("a2a codec: ~0p", [Reason]))}}
    end.
