-module(livery_grpc_a2a_SUITE).
-moduledoc """
End to end: one barrel_a2a agent served over gRPC by
`livery_grpc_a2a`, driven three ways.

The `client` group uses `barrel_a2a_client` with
`livery_grpc_a2a_client` registered for the `GRPC` binding, so the whole
A2A client stack runs over gRPC. The `wire` group uses a raw
`livery_grpc_client` to pin the bytes: which gRPC status an A2A error
becomes, and that the `google.rpc.Status` in `grpc-status-details-bin`
names the A2A reason. The `grpcurl` group proves an external, non-Erlang
client can drive the service; it skips when grpcurl is absent.

The agent is `a2a_agent` in this file, a reimplementation of
barrel_a2a's own test agent, so the binding is exercised against
behaviour this repository controls.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 10000).

all() ->
    [{group, client}, {group, wire}, {group, grpcurl}, {group, python}].

groups() ->
    [
        {client, [], [
            t_send,
            t_send_direct_message,
            t_streaming_send,
            t_subscribe_to_task,
            t_input_required_follow_up,
            t_cancel,
            t_cancel_stream,
            t_list_tasks,
            t_extended_card,
            t_push_config,
            t_version_error,
            t_task_not_found,
            t_binding_is_grpc
        ]},
        {wire, [], [
            t_wire_unary,
            t_wire_error_details,
            t_wire_server_stream,
            t_wire_unknown_method,
            t_wire_deadline
        ]},
        {grpcurl, [], [t_grpcurl_list, t_grpcurl_send, t_grpcurl_error]},
        {python, [], [
            t_python_send,
            t_python_stream,
            t_python_multiturn,
            t_python_cancel,
            t_python_get,
            t_python_direct,
            t_python_error
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(livery_grpc),
    {ok, _} = application:ensure_all_started(barrel_a2a),
    Port = free_port(),
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_binary(Port)]),
    Card = card(Url),
    {ok, Agent} = barrel_a2a_server:start(Card, #{
        handler => a2a_agent,
        listen => false,
        blocking_timeout => 5000,
        push_notifications => true,
        extended_card => extended_card(Url)
    }),
    {ok, Server} = livery_grpc:start_server(#{
        port => Port,
        reflection => true,
        services => [livery_grpc_a2a:service(Agent)]
    }),
    [
        {agent, Agent},
        {server, Server},
        {port, Port},
        {url, Url},
        %% The published card carries the signatures and capabilities the
        %% server filled in; that is what a client would fetch.
        {card, barrel_a2a_server:card(Agent)}
        | Config
    ].

end_per_suite(Config) ->
    ok = livery_grpc:stop_server(?config(server, Config)),
    ok = barrel_a2a_server:stop(?config(agent, Config)).

init_per_group(grpcurl, Config) ->
    case os:find_executable("grpcurl") of
        false -> {skip, "grpcurl not installed"};
        Path -> [{grpcurl, Path}, {proto_args, proto_args()} | Config]
    end;
init_per_group(python, Config) ->
    case python_interpreter() of
        false ->
            {skip, "set INTEROP_PYTHON, or run `make interop-a2a-setup`"};
        Python ->
            [{python, Python}, {script, interop_script()} | Config]
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

%%====================================================================
%% The agent
%%====================================================================

%% The interface a client selects. `tenant` is left unset: this server
%% serves one agent.
card(Url) ->
    barrel_a2a_agent_card:new(#{
        name => <<"gRPC Test Agent">>,
        description => <<"Agent used by the livery_grpc_a2a suite">>,
        version => <<"1.2.3">>,
        default_input_modes => [<<"text/plain">>, <<"application/json">>],
        default_output_modes => [<<"text/plain">>, <<"application/json">>],
        supported_interfaces => [
            barrel_a2a_agent_card:interface(Url, <<"GRPC">>, <<"1.0">>)
        ],
        skills => [
            #{
                id => <<"echo">>,
                name => <<"Echo">>,
                description => <<"Echoes text back">>,
                tags => [<<"test">>]
            }
        ]
    }).

extended_card(Url) ->
    Card = card(Url),
    Card#{<<"name">> => <<"gRPC Test Agent (extended)">>}.

%%====================================================================
%% barrel_a2a_client over gRPC
%%====================================================================

t_send(Config) ->
    Agent = connect(Config),
    {ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"echo: over grpc">>),
    ?assertEqual(completed, barrel_a2a_task:state(Task)),
    [Artifact] = barrel_a2a_task:artifacts(Task),
    ?assertEqual(<<"over grpc">>, barrel_a2a_artifact:text(Artifact)),
    %% The reply is a valid A2A object, not merely one this codec accepts.
    ?assertEqual(ok, barrel_a2a_schema:validate(<<"Task">>, Task)),
    ok = barrel_a2a_client:close(Agent).

t_send_direct_message(Config) ->
    Agent = connect(Config),
    {ok, {message, Message}} = barrel_a2a_client:send(Agent, <<"direct">>),
    ?assertEqual(<<"direct reply">>, barrel_a2a_message:text(Message)),
    ?assertEqual(ok, barrel_a2a_schema:validate(<<"Message">>, Message)),
    ok = barrel_a2a_client:close(Agent).

t_streaming_send(Config) ->
    Agent = connect(Config),
    {ok, Task} = barrel_a2a_client:start(Agent, <<"stream">>),
    ok = barrel_a2a_remote_task:stream_to(Task, self()),
    {Events, {done, Final}} = collect(Task),
    ?assertEqual(
        [task, status_update, artifact_update, artifact_update, status_update], kinds(Events)
    ),
    ?assertEqual([working, completed], states(Events)),
    [#{<<"artifactUpdate">> := First}, #{<<"artifactUpdate">> := Second}] =
        [E || E <- Events, barrel_a2a_event:kind(E) =:= artifact_update],
    %% `append` and `lastChunk` are plain proto3 booleans, so `false` is
    %% the absent value on the wire and in the JSON.
    ?assertEqual(false, maps:get(<<"append">>, First, false)),
    ?assertEqual(true, maps:get(<<"append">>, Second)),
    ?assertEqual(true, maps:get(<<"lastChunk">>, Second)),
    ?assertEqual(completed, barrel_a2a_task:state(Final)),
    [Artifact] = barrel_a2a_task:artifacts(Final),
    ?assertEqual(<<"part one part two">>, barrel_a2a_artifact:text(Artifact)),
    lists:foreach(
        fun(E) -> ?assertEqual(ok, barrel_a2a_schema:validate(<<"StreamResponse">>, E)) end,
        Events
    ),
    ok = barrel_a2a_client:close(Agent).

t_subscribe_to_task(Config) ->
    Agent = connect(Config),
    {ok, {task, Started}} = barrel_a2a_client:send(Agent, <<"slow 600">>, #{
        return_immediately => true
    }),
    Id = barrel_a2a_task:id(Started),
    {ok, Task} = barrel_a2a_client:subscribe(Agent, Id),
    ok = barrel_a2a_remote_task:stream_to(Task, self()),
    {Events, {done, Final}} = collect(Task),
    ?assertEqual(task, hd(kinds(Events))),
    ?assertEqual(completed, barrel_a2a_task:state(Final)),
    %% A task already in a terminal state cannot be subscribed to.
    ?assertMatch(
        {error, #{type := unsupported_operation}},
        barrel_a2a_client:call(Agent, subscribe_to_task, #{<<"id">> => Id})
    ),
    ok = barrel_a2a_client:close(Agent).

t_input_required_follow_up(Config) ->
    Agent = connect(Config),
    {ok, {task, Paused}} = barrel_a2a_client:send(Agent, <<"ask">>),
    ?assertEqual(input_required, barrel_a2a_task:state(Paused)),
    Id = barrel_a2a_task:id(Paused),
    {ok, {task, Done}} = barrel_a2a_client:send(Agent, <<"second">>, #{task_id => Id}),
    ?assertEqual(completed, barrel_a2a_task:state(Done)),
    ?assertEqual(Id, barrel_a2a_task:id(Done)),
    [Artifact] = barrel_a2a_task:artifacts(Done),
    ?assertEqual(<<"thanks: second">>, barrel_a2a_artifact:text(Artifact)),
    ok = barrel_a2a_client:close(Agent).

t_cancel(Config) ->
    Agent = connect(Config),
    {ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"cancel-me">>, #{
        return_immediately => true
    }),
    Id = barrel_a2a_task:id(Task),
    {ok, Cancelled} = barrel_a2a_client:cancel(Agent, Id),
    ?assertEqual(canceled, barrel_a2a_task:state(Cancelled)),
    {ok, Read} = barrel_a2a_client:get_task(Agent, Id),
    ?assertEqual(canceled, barrel_a2a_task:state(Read)),
    ok = barrel_a2a_client:close(Agent).

%% Cancelling a stream resets it, so the agent sees the peer go away and
%% the owner gets exactly one terminal message.
t_cancel_stream(Config) ->
    Agent = connect(Config),
    Request = barrel_a2a_client:send_request(Agent, <<"stream-slow 3000">>, #{}),
    {ok, Ref} = barrel_a2a_client:stream(Agent, send_streaming_message, Request, self()),
    receive
        {a2a_stream, Ref, {event, _Event}} -> ok
    after ?TIMEOUT -> ct:fail(no_first_event)
    end,
    Started = erlang:monotonic_time(millisecond),
    ok = barrel_a2a_client:cancel_stream(Agent, Ref),
    %% Events already in flight are still delivered; the terminal message
    %% is `done`, and it is the last thing the owner sees.
    ok = drain_to_terminal(Ref),
    receive
        {a2a_stream, Ref, After} -> ct:fail({message_after_terminal, After})
    after 300 -> ok
    end,
    %% The agent is still busy for 3s: the stream ended because it was
    %% cancelled, not because the task finished.
    ?assert(erlang:monotonic_time(millisecond) - Started < 2000),
    ok = barrel_a2a_client:close(Agent).

drain_to_terminal(Ref) ->
    receive
        {a2a_stream, Ref, done} -> ok;
        {a2a_stream, Ref, {error, Error}} -> ct:fail({unexpected_error, Error});
        {a2a_stream, Ref, {event, _Event}} -> drain_to_terminal(Ref)
    after ?TIMEOUT -> ct:fail(no_terminal_message)
    end.

t_list_tasks(Config) ->
    Agent = connect(Config),
    ContextId = barrel_a2a_id:uuid(),
    {ok, {task, _}} = barrel_a2a_client:send(Agent, <<"echo: a">>, #{context_id => ContextId}),
    {ok, {task, _}} = barrel_a2a_client:send(Agent, <<"echo: b">>, #{context_id => ContextId}),
    {ok, #{tasks := Tasks, total_size := Total}} =
        barrel_a2a_client:list_tasks(Agent, #{context_id => ContextId}),
    ?assertEqual(2, length(Tasks)),
    ?assertEqual(2, Total),
    lists:foreach(
        fun(T) -> ?assertEqual(ContextId, barrel_a2a_task:context_id(T)) end,
        Tasks
    ),
    %% An empty page still answers with `tasks`, which the A2A schema
    %% marks required.
    ?assertMatch(
        {ok, #{tasks := []}},
        barrel_a2a_client:list_tasks(Agent, #{context_id => barrel_a2a_id:uuid()})
    ),
    ok = barrel_a2a_client:close(Agent).

%% barrel_a2a 0.2.0 refuses the extended card to an anonymous caller
%% (spec 13.3), and this suite configures no authentication, so the
%% refusal is what the binding has to carry. Serving the card instead
%% means giving the server an `auth' scheme and having the binding put
%% the authenticated peer in the request context as `principal'.
t_extended_card(Config) ->
    Agent = connect(Config),
    ?assertMatch(
        {error, #{type := unauthenticated}},
        barrel_a2a_client:extended_card(Agent)
    ),
    ok = barrel_a2a_client:close(Agent).

t_push_config(Config) ->
    Agent = connect(Config),
    {ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"echo: push">>),
    Id = barrel_a2a_task:id(Task),
    {ok, Created} = barrel_a2a_client:create_push_config(Agent, Id, #{
        url => <<"https://hooks.example/notify">>
    }),
    ConfigId = maps:get(<<"id">>, Created),
    {ok, Read} = barrel_a2a_client:get_push_config(Agent, Id, ConfigId),
    ?assertEqual(<<"https://hooks.example/notify">>, maps:get(<<"url">>, Read)),
    {ok, #{configs := [_]}} = barrel_a2a_client:list_push_configs(Agent, Id, #{}),
    ok = barrel_a2a_client:delete_push_config(Agent, Id, ConfigId),
    ?assertMatch({ok, #{configs := []}}, barrel_a2a_client:list_push_configs(Agent, Id, #{})),
    ok = barrel_a2a_client:close(Agent).

%% Version negotiation happens in the engine, so the gRPC binding must
%% carry `A2A-Version` as metadata for it to see.
t_version_error(Config) ->
    Agent = connect(Config, #{version => <<"9.9">>}),
    ?assertMatch(
        {error, #{type := version_not_supported}},
        barrel_a2a_client:send(Agent, <<"echo: nope">>)
    ),
    ok = barrel_a2a_client:close(Agent).

t_task_not_found(Config) ->
    Agent = connect(Config),
    {error, Error} = barrel_a2a_client:get_task(Agent, <<"no-such-task">>),
    ?assertEqual(task_not_found, barrel_a2a_error:type(Error)),
    ok = barrel_a2a_client:close(Agent).

t_binding_is_grpc(Config) ->
    Agent = connect(Config),
    ?assertEqual(<<"GRPC">>, barrel_a2a_client:binding(Agent)),
    ok = barrel_a2a_client:close(Agent).

%%====================================================================
%% Raw gRPC: the bytes on the wire
%%====================================================================

t_wire_unary(Config) ->
    with_conn(Config, fun(Conn) ->
        Request = #{
            message => #{
                message_id => barrel_a2a_id:uuid(),
                role => 'ROLE_USER',
                parts => [#{content => {text, <<"echo: raw">>}}]
            }
        },
        {ok, Reply} = grpc_call(Conn, 'SendMessage', Request),
        #{payload := {task, Task}} = Reply,
        ?assertEqual('TASK_STATE_COMPLETED', maps:get(state, maps:get(status, Task))),
        [#{parts := [#{content := {text, Text}}]}] = maps:get(artifacts, Task),
        ?assertEqual(<<"raw">>, Text)
    end).

%% An A2A error becomes the gRPC status barrel_a2a_error maps it to, and
%% the details carry an ErrorInfo naming the A2A reason.
t_wire_error_details(Config) ->
    with_conn(Config, fun(Conn) ->
        Result = grpc_call(Conn, 'GetTask', #{id => <<"missing">>}),
        {error, {Status, Message, Details}} = Result,
        ?assertEqual(not_found, Status),
        ?assertNotEqual(<<>>, Message),
        Decoded = a2a_status_pb:decode_msg(Details, 'Status'),
        ?assertEqual(livery_grpc_status:code(not_found), maps:get(code, Decoded)),
        [#{type_url := TypeUrl, value := Value}] = maps:get(details, Decoded),
        ?assertEqual(<<"type.googleapis.com/google.rpc.ErrorInfo">>, TypeUrl),
        Info = a2a_status_pb:decode_msg(Value, 'ErrorInfo'),
        ?assertEqual(<<"TASK_NOT_FOUND">>, maps:get(reason, Info)),
        ?assertEqual(<<"a2a-protocol.org">>, maps:get(domain, Info))
    end).

t_wire_server_stream(Config) ->
    with_conn(Config, fun(Conn) ->
        {ok, Method} = livery_grpc_service:method(a2a_pb, 'A2AService', 'SendStreamingMessage'),
        Request = #{
            message => #{
                message_id => barrel_a2a_id:uuid(),
                role => 'ROLE_USER',
                parts => [#{content => {text, <<"stream">>}}]
            }
        },
        {ok, Messages} = livery_grpc_client:call(Conn, Method, Request, call_opts()),
        Payloads = [P || #{payload := P} <- Messages],
        Kinds = [element(1, P) || P <- Payloads],
        ?assertEqual(
            [task, status_update, artifact_update, artifact_update, status_update], Kinds
        )
    end).

t_wire_unknown_method(Config) ->
    with_conn(Config, fun(Conn) ->
        Method = #{
            name => 'Nope',
            function => nope,
            path => <<"/lf.a2a.v1.A2AService/Nope">>,
            proto => a2a_pb,
            service => 'A2AService',
            input => 'GetTaskRequest',
            output => 'Task',
            input_stream => false,
            output_stream => false,
            kind => unary
        },
        ?assertMatch(
            {error, {unimplemented, _}},
            livery_grpc_client:call(Conn, Method, #{id => <<"x">>}, call_opts())
        )
    end).

%% grpc-timeout reaches the handler, and a call that outlives it comes
%% back as deadline_exceeded rather than hanging.
t_wire_deadline(Config) ->
    with_conn(Config, fun(Conn) ->
        Request = #{
            message => #{
                message_id => barrel_a2a_id:uuid(),
                role => 'ROLE_USER',
                parts => [#{content => {text, <<"slow 3000">>}}]
            }
        },
        {ok, Method} = livery_grpc_service:method(a2a_pb, 'A2AService', 'SendMessage'),
        Opts = (call_opts())#{deadline => 400, timeout => 5000},
        ?assertMatch(
            {error, {deadline_exceeded, _}},
            livery_grpc_client:call(Conn, Method, Request, Opts)
        )
    end).

%%====================================================================
%% grpcurl: a real external client
%%====================================================================

%% Reflection advertises the service. It cannot serve a2a_pb's message
%% schemas: gpb's descriptor output omits the synthetic map entry types
%% (`Struct.FieldsEntry` and friends), which protoreflect rejects. The
%% calls below therefore pass the vendored `.proto` instead, which is
%% what an external client would have anyway.
t_grpcurl_list(Config) ->
    Out = grpcurl(Config, "", "list"),
    ?assert(contains(Out, "lf.a2a.v1.A2AService")).

t_grpcurl_send(Config) ->
    Payload =
        "-d '{\"message\":{\"messageId\":\"grpcurl-1\",\"role\":\"ROLE_USER\","
        "\"parts\":[{\"text\":\"echo: from grpcurl\"}]}}'",
    Out = grpcurl(
        Config, ?config(proto_args, Config) ++ Payload, "lf.a2a.v1.A2AService/SendMessage"
    ),
    ?assert(contains(Out, "from grpcurl")),
    ?assert(contains(Out, "TASK_STATE_COMPLETED")).

t_grpcurl_error(Config) ->
    Args = ?config(proto_args, Config) ++ "-d '{\"id\":\"missing\"}'",
    Out = grpcurl(Config, Args, "lf.a2a.v1.A2AService/GetTask"),
    ?assert(contains(Out, "NotFound")),
    ?assert(contains(Out, "TASK_NOT_FOUND")).

%%====================================================================
%% The official A2A Python SDK, over its gRPC transport
%%====================================================================

%% Blocking SendMessage answers with one completed Task.
t_python_send(Config) ->
    [Step] = python(Config, "send"),
    ?assertEqual([<<"task">>], maps:get(<<"kinds">>, Step)),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, maps:get(<<"state">>, Step)),
    ?assertEqual(<<"from python">>, maps:get(<<"text">>, Step)).

%% SendStreamingMessage: the SDK sees the event order and reassembles the
%% artifact from its two chunks.
t_python_stream(Config) ->
    [Step] = python(Config, "stream"),
    ?assertEqual(
        [
            <<"task">>,
            <<"status_update">>,
            <<"artifact_update">>,
            <<"artifact_update">>,
            <<"status_update">>
        ],
        maps:get(<<"kinds">>, Step)
    ),
    ?assertEqual(
        [<<"TASK_STATE_WORKING">>, <<"TASK_STATE_COMPLETED">>], maps:get(<<"states">>, Step)
    ),
    ?assertEqual(<<"part one part two">>, maps:get(<<"text">>, Step)).

t_python_multiturn(Config) ->
    [Ask, FollowUp] = python(Config, "multiturn"),
    ?assertEqual(<<"TASK_STATE_INPUT_REQUIRED">>, maps:get(<<"state">>, Ask)),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, maps:get(<<"state">>, FollowUp)),
    ?assertEqual(maps:get(<<"task_id">>, Ask), maps:get(<<"task_id">>, FollowUp)),
    ?assertEqual(<<"thanks: second">>, maps:get(<<"text">>, FollowUp)).

t_python_cancel(Config) ->
    [_Started, Cancel, Get] = python(Config, "cancel"),
    ?assertEqual(<<"TASK_STATE_CANCELED">>, maps:get(<<"state">>, Cancel)),
    ?assertEqual(<<"TASK_STATE_CANCELED">>, maps:get(<<"state">>, Get)).

t_python_get(Config) ->
    [Step] = python(Config, "get"),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, maps:get(<<"state">>, Step)),
    ?assertEqual(<<"fetch me">>, maps:get(<<"text">>, Step)),
    ?assert(maps:get(<<"same_id">>, Step)).

t_python_direct(Config) ->
    [Step] = python(Config, "direct"),
    ?assertEqual([<<"message">>], maps:get(<<"kinds">>, Step)),
    ?assertEqual(<<"direct reply">>, maps:get(<<"text">>, Step)).

%% The SDK recovers the A2A error type from the ErrorInfo this binding
%% puts in grpc-status-details-bin, rather than seeing a bare NOT_FOUND.
t_python_error(Config) ->
    [Step] = python(Config, "error"),
    ?assertEqual(<<"TaskNotFoundError">>, maps:get(<<"error">>, Step)).

%% Run one scenario and return the JSON objects it printed.
python(Config, Scenario) ->
    Command = lists:flatten(
        io_lib:format(
            "~s ~s 127.0.0.1:~b ~s 2>&1; echo \"exit=$?\"",
            [?config(python, Config), ?config(script, Config), ?config(port, Config), Scenario]
        )
    ),
    Out = os:cmd(Command),
    ct:pal("~s~n~s", [Command, Out]),
    Lines = string:lexemes(Out, "\n"),
    case lists:last(Lines) of
        "exit=0" -> ok;
        Other -> ct:fail({python_failed, Other})
    end,
    [json:decode(iolist_to_binary(L)) || L <- Lines, string:prefix(L, "{") =/= nomatch].

python_interpreter() ->
    Candidates = [os:getenv("INTEROP_PYTHON"), venv_python()],
    case [P || P <- Candidates, P =/= false, filelib:is_regular(P)] of
        [Python | _] -> Python;
        [] -> false
    end.

venv_python() -> interop_path(".venv/bin/python").

interop_script() -> interop_path("a2a_grpc_client.py").

%% The suite runs from the CT log directory, so interop paths are
%% resolved against the source tree.
interop_path(Relative) ->
    filename:join([source_root(), "test", "interop", Relative]).

%%====================================================================
%% Helpers
%%====================================================================

connect(Config) -> connect(Config, #{}).

connect(Config, Extra) ->
    Opts = maps:merge(
        #{
            transports => [{<<"GRPC">>, livery_grpc_a2a_client}],
            prefer => [grpc],
            timeout => ?TIMEOUT
        },
        Extra
    ),
    {ok, Agent} = barrel_a2a_client:from_card(?config(card, Config), Opts),
    Agent.

with_conn(Config, Fun) ->
    {ok, Conn} = livery_grpc_client:connect("127.0.0.1", ?config(port, Config)),
    try
        Fun(Conn)
    after
        livery_grpc_client:close(Conn)
    end.

grpc_call(Conn, Rpc, Request) ->
    {ok, Method} = livery_grpc_service:method(a2a_pb, 'A2AService', Rpc),
    livery_grpc_client:call(Conn, Method, Request, call_opts()).

call_opts() ->
    #{timeout => ?TIMEOUT, metadata => [{<<"a2a-version">>, <<"1.0">>}]}.

collect(Task) -> collect(Task, []).

collect(Task, Acc) ->
    receive
        {a2a_event, Task, Event} -> collect(Task, [Event | Acc]);
        {a2a_done, Task, Final} -> {lists:reverse(Acc), {done, Final}};
        {a2a_error, Task, Error} -> {lists:reverse(Acc), {error, Error}}
    after ?TIMEOUT -> {lists:reverse(Acc), timeout}
    end.

kinds(Events) -> [barrel_a2a_event:kind(E) || E <- Events].

states(Events) ->
    [
        State
     || #{<<"statusUpdate">> := #{<<"status">> := #{<<"state">> := Wire}}} <- Events,
        {ok, State} <- [barrel_a2a_task_state:from_wire(Wire)]
    ].

free_port() ->
    {ok, Listen} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Port.

%% The suite runs from the CT log directory, so grpcurl needs an absolute
%% import path.
proto_args() ->
    "-import-path " ++ filename:join(source_root(), "proto") ++
        " -proto a2a.proto -proto google/rpc/error_details.proto"
        " -H 'a2a-version: 1.0' ".

%% `src` in the built app is a symlink to the source tree, whose parent
%% is the repository root.
%% This application's own source tree: `proto/a2a.proto' ships here,
%% not in livery_grpc.
source_root() ->
    Lib = code:lib_dir(livery_grpc_a2a),
    case file:read_link_all(filename:join(Lib, "src")) of
        {ok, Link} -> filename:dirname(filename:absname(Link, Lib));
        {error, _} -> filename:absname(filename:join(Lib, ".."))
    end.

grpcurl(Config, Args, Target) ->
    Command = lists:flatten(
        io_lib:format(
            "~s -plaintext ~s 127.0.0.1:~b ~s 2>&1",
            [?config(grpcurl, Config), Args, ?config(port, Config), Target]
        )
    ),
    Out = os:cmd(Command),
    ct:pal("~s~n~s", [Command, Out]),
    Out.

contains(Haystack, Needle) ->
    string:find(Haystack, Needle) =/= nomatch.
