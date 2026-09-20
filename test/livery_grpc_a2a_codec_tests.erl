-module(livery_grpc_a2a_codec_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Vector corpus
%%====================================================================

%% Every schema vector must survive `to_pb` then `to_json`. One vector
%% is skipped, for the reason barrel_a2a's own vector suite skips it.
%%
%% The vectors are vendored under test/schema_vectors (see its
%% VENDORED.md). They used to be read from barrel_a2a's own test
%% directory, which a hex install does not ship: that worked from a
%% sibling checkout and silently contributed nothing in CI. A missing
%% directory is a failure now, not an empty list.
vectors_test_() ->
    {ok, Dir} = vector_dir(),
    Vectors = vectors(Dir),
    ?assertNotEqual([], Vectors),
    {inparallel, [vector_case(Type, File) || {Type, File} <- Vectors]}.

vector_case(Type, File) ->
    {atom_to_list(Type) ++ "/" ++ filename:basename(File), fun() -> round_trip(Type, File) end}.

%% The round trip proves the codec is self-consistent. This proves it is
%% right: what it writes is checked against the official A2A JSON Schema
%% bundle, which barrel_a2a ships in `priv' and therefore travels with
%% the dependency rather than with these vectors.
vectors_match_the_schema_test_() ->
    {ok, Dir} = vector_dir(),
    Types = barrel_a2a_schema:types(),
    {inparallel, [
        {atom_to_list(Type) ++ "/" ++ filename:basename(File) ++ " matches the schema", fun() ->
            schema_check(Type, File)
        end}
     || {Type, File} <- vectors(Dir),
        lists:member(atom_to_binary(Type, utf8), Types)
    ]}.

schema_check(Type, File) ->
    Out = livery_grpc_a2a_codec:to_json(Type, livery_grpc_a2a_codec:to_pb(Type, decode_file(File))),
    ?assertEqual(ok, barrel_a2a_schema:validate(atom_to_binary(Type, utf8), Out)).

round_trip(Type, File) ->
    In = decode_file(File),
    Pb = livery_grpc_a2a_codec:to_pb(Type, In),
    Out = livery_grpc_a2a_codec:to_json(Type, Pb),
    %% The JSON round trip loses nothing: re-reading the output yields
    %% the same message on the wire.
    ?assertEqual(canonical(Type, Pb), canonical(Type, livery_grpc_a2a_codec:to_pb(Type, Out))),
    %% Nothing lost: everything in the input was written back, except
    %% the fields whose value is a protobuf default and which the codec
    %% therefore omits.
    ?assert(covered_by_modulo_defaults(In, Out)),
    %% Nothing invented: everything written was in the input, except the
    %% empty arrays the codec writes where a vector left them out.
    ?assert(covered_by_modulo_defaults(Out, In)).

%% Round tripping through the wire canonicalises field presence: gpb
%% fills in every implicit-presence field on decode, so a map that omits
%% one and a map that carries its default compare equal.
canonical(Type, Pb) ->
    Proto = livery_grpc_a2a_codec:proto(),
    Proto:decode_msg(Proto:encode_msg(Pb, Type), Type).

covered_by(A, B) when is_map(A), is_map(B) ->
    lists:all(
        fun({Key, Value}) ->
            case maps:find(Key, B) of
                {ok, Other} -> covered_by(Value, Other);
                error -> false
            end
        end,
        maps:to_list(A)
    );
covered_by(A, B) when is_list(A), is_list(B), length(A) =:= length(B) ->
    lists:all(fun({X, Y}) -> covered_by(X, Y) end, lists:zip(A, B));
covered_by(A, B) when is_number(A), is_number(B) ->
    A == B;
covered_by(A, B) when is_binary(A), is_binary(B) ->
    A =:= B orelse same_instant(A, B);
covered_by(A, B) ->
    A =:= B.

covered_by_modulo_defaults(A, B) when is_map(A), is_map(B) ->
    lists:all(
        fun({Key, Value}) ->
            is_protobuf_default(Value) orelse
                case maps:find(Key, B) of
                    {ok, Other} -> covered_by_modulo_defaults(Value, Other);
                    error -> false
                end
        end,
        maps:to_list(A)
    );
covered_by_modulo_defaults(A, B) when is_list(A), is_list(B), length(A) =:= length(B) ->
    lists:all(fun({X, Y}) -> covered_by_modulo_defaults(X, Y) end, lists:zip(A, B));
covered_by_modulo_defaults(A, B) ->
    covered_by(A, B).

is_protobuf_default(<<>>) -> true;
is_protobuf_default(false) -> true;
is_protobuf_default([]) -> true;
is_protobuf_default(N) when is_number(N) -> N == 0;
is_protobuf_default(M) when is_map(M) -> map_size(M) =:= 0;
is_protobuf_default(_) -> false.

%% Timestamps come back at the millisecond precision barrel_a2a uses, so
%% a vector written with more digits is compared as an instant.
same_instant(A, B) ->
    case barrel_a2a_time:from_iso(A) of
        error -> false;
        Instant -> Instant =:= barrel_a2a_time:from_iso(B)
    end.

%% The A2A 1.0.1 schema names an AgentCard's requirements
%% `securityRequirements` with `{schemes: {Name: {list: [Scopes]}}}`,
%% which is what the proto says too. The specification's own section 8.5
%% sample card instead uses `security` with `{Name: [Scopes]}`.
%% barrel_a2a's vector suite skips it for the same reason.
skipped() ->
    [{'AgentCard', "03.json"}].

vectors(Dir) ->
    [
        {Type, File}
     || TypeDir <- filelib:wildcard(filename:join(Dir, "*")),
        filelib:is_dir(TypeDir),
        Type <- [list_to_atom(filename:basename(TypeDir))],
        File <- filelib:wildcard(filename:join(TypeDir, "*.json")),
        not lists:member({Type, filename:basename(File)}, skipped())
    ].

decode_file(File) ->
    {ok, Bin} = file:read_file(File),
    json:decode(Bin).

%% The vendored vectors, overridable for a bisect against another
%% revision of the specification.
vector_dir() ->
    Candidates =
        [os:getenv("A2A_SCHEMA_VECTORS")] ++
            [
                filename:join([Root, "test", "schema_vectors", "1.0.1", "examples"])
             || Root <- [source_root(), "."], Root =/= false
            ],
    case [D || D <- Candidates, D =/= false, filelib:is_dir(D)] of
        [Dir | _] -> {ok, Dir};
        [] -> error
    end.

%% This application's own source tree, where the vendored vectors live.
source_root() ->
    case code:lib_dir(livery_grpc_a2a) of
        {error, _} ->
            false;
        Dir ->
            case file:read_link_all(filename:join(Dir, "src")) of
                {ok, Link} -> filename:dirname(filename:absname(Link, Dir));
                {error, _} -> filename:absname(filename:join(Dir, ".."))
            end
    end.

%%====================================================================
%% Shapes the vectors do not reach
%%====================================================================

%% An implicit-presence field at its default is omitted; an explicit
%% `optional` field is written even when it holds the same value.
presence_test() ->
    Json = livery_grpc_a2a_codec:to_json('GetTaskRequest', #{
        tenant => <<>>, id => <<"t1">>, history_length => 0
    }),
    ?assertEqual(#{<<"id">> => <<"t1">>, <<"historyLength">> => 0}, Json),
    ?assertEqual(
        #{<<"id">> => <<"t1">>},
        livery_grpc_a2a_codec:to_json('GetTaskRequest', #{tenant => <<>>, id => <<"t1">>})
    ).

%% A ProtoJSON parser accepts the proto field name as well as the JSON
%% one.
proto_field_name_test() ->
    ?assertEqual(
        #{id => <<"t1">>, history_length => 5},
        livery_grpc_a2a_codec:to_pb('GetTaskRequest', #{
            <<"id">> => <<"t1">>, <<"history_length">> => 5
        })
    ).

oneof_test() ->
    Pb = livery_grpc_a2a_codec:to_pb('StreamResponse', #{
        <<"statusUpdate">> => #{
            <<"taskId">> => <<"t1">>,
            <<"contextId">> => <<"c1">>,
            <<"status">> => #{<<"state">> => <<"TASK_STATE_COMPLETED">>}
        }
    }),
    ?assertMatch(#{payload := {status_update, #{task_id := <<"t1">>}}}, Pb),
    #{payload := {status_update, #{status := Status}}} = Pb,
    ?assertEqual(#{state => 'TASK_STATE_COMPLETED'}, Status),
    ?assertMatch(
        #{<<"statusUpdate">> := #{<<"status">> := #{<<"state">> := <<"TASK_STATE_COMPLETED">>}}},
        livery_grpc_a2a_codec:to_json('StreamResponse', Pb)
    ).

unknown_enum_value_test() ->
    ?assertError(
        {a2a_codec, {unknown_enum_value, _, <<"NOPE">>}},
        livery_grpc_a2a_codec:to_pb('TaskStatus', #{<<"state">> => <<"NOPE">>})
    ).

%% google.protobuf.Value carries any JSON, and `null` inside one is the
%% null value rather than an absent field.
struct_and_value_test() ->
    Json = #{
        <<"data">> => #{
            <<"n">> => 1,
            <<"f">> => 1.5,
            <<"s">> => <<"x">>,
            <<"b">> => true,
            <<"nil">> => null,
            <<"list">> => [1, <<"two">>, [], #{}]
        }
    },
    Pb = livery_grpc_a2a_codec:to_pb('Part', Json),
    ?assertMatch(#{content := {data, #{kind := {struct_value, _}}}}, Pb),
    ?assertEqual(Json, livery_grpc_a2a_codec:to_json('Part', Pb)).

%% An empty Struct is a present message, not an absent field, so it
%% survives the round trip.
empty_struct_test() ->
    Json = #{<<"text">> => <<"hi">>, <<"metadata">> => #{}},
    Pb = livery_grpc_a2a_codec:to_pb('Part', Json),
    ?assertEqual(Json, livery_grpc_a2a_codec:to_json('Part', Pb)).

%% A `null` outside a Value means "not set".
null_is_unset_test() ->
    ?assertEqual(
        #{content => {text, <<"hi">>}},
        livery_grpc_a2a_codec:to_pb('Part', #{<<"text">> => <<"hi">>, <<"metadata">> => null})
    ).

bytes_test() ->
    Padded = livery_grpc_a2a_codec:to_pb('Part', #{<<"raw">> => <<"aGVsbG8=">>}),
    ?assertEqual(#{content => {raw, <<"hello">>}}, Padded),
    %% Unpadded and URL-safe input is accepted; output is padded standard.
    ?assertEqual(
        #{content => {raw, <<255, 254, 253>>}},
        livery_grpc_a2a_codec:to_pb('Part', #{<<"raw">> => <<"__79">>})
    ),
    ?assertEqual(
        #{<<"raw">> => <<"aGVsbG8=">>}, livery_grpc_a2a_codec:to_json('Part', Padded)
    ).

timestamp_test() ->
    Pb = livery_grpc_a2a_codec:to_pb('TaskStatus', #{
        <<"state">> => <<"TASK_STATE_COMPLETED">>,
        <<"timestamp">> => <<"2024-03-15T10:15:00Z">>
    }),
    ?assertEqual(#{seconds => 1710497700, nanos => 0}, maps:get(timestamp, Pb)),
    %% Output carries the millisecond precision every A2A binding uses.
    ?assertMatch(
        #{<<"timestamp">> := <<"2024-03-15T10:15:00.000Z">>},
        livery_grpc_a2a_codec:to_json('TaskStatus', Pb)
    ).

invalid_timestamp_test() ->
    ?assertError(
        {a2a_codec, {invalid_timestamp, <<"yesterday">>}},
        livery_grpc_a2a_codec:to_pb('TaskStatus', #{<<"timestamp">> => <<"yesterday">>})
    ).

%% A protobuf map field is an object keyed by the scheme name.
map_field_test() ->
    Json = #{<<"schemes">> => #{<<"apiKey">> => #{<<"list">> => [<<"read">>]}}},
    Pb = livery_grpc_a2a_codec:to_pb('SecurityRequirement', Json),
    ?assertEqual(#{schemes => #{<<"apiKey">> => #{list => [<<"read">>]}}}, Pb),
    ?assertEqual(Json, livery_grpc_a2a_codec:to_json('SecurityRequirement', Pb)).

%% A repeated field is written even when empty, because the A2A schema
%% marks several arrays required.
empty_repeated_test() ->
    ?assertEqual(
        #{<<"tasks">> => [], <<"pageSize">> => 10, <<"totalSize">> => 3},
        livery_grpc_a2a_codec:to_json('ListTasksResponse', #{
            tasks => [], next_page_token => <<>>, page_size => 10, total_size => 3
        })
    ),
    %% An empty protobuf map is still omitted, as ProtoJSON says.
    ?assertEqual(
        #{},
        livery_grpc_a2a_codec:to_json('SecurityRequirement', #{schemes => #{}})
    ).

empty_message_test() ->
    ?assertEqual(#{}, livery_grpc_a2a_codec:to_json('Empty', #{})),
    ?assertEqual(#{}, livery_grpc_a2a_codec:to_pb('Empty', #{})).

unknown_json_key_is_ignored_test() ->
    ?assertEqual(
        #{id => <<"t1">>},
        livery_grpc_a2a_codec:to_pb('GetTaskRequest', #{
            <<"id">> => <<"t1">>, <<"somethingElse">> => 42
        })
    ).

%%====================================================================
%% Errors
%%====================================================================

%% The gRPC status, the message and the ErrorInfo reason all come from
%% the A2A error type.
error_status_test() ->
    Error = barrel_a2a_error:new(task_not_found, <<"no such task">>),
    {Status, Message, Details} = livery_grpc_a2a_codec:error_status(Error),
    ?assertEqual(not_found, Status),
    ?assertEqual(<<"no such task">>, Message),
    Decoded = (livery_grpc_a2a_codec:status_proto()):decode_msg(Details, 'Status'),
    ?assertEqual(livery_grpc_status:code(not_found), maps:get(code, Decoded)),
    ?assertEqual(<<"no such task">>, maps:get(message, Decoded)),
    ?assertMatch(
        [#{type_url := <<"type.googleapis.com/google.rpc.ErrorInfo">>}],
        maps:get(details, Decoded)
    ).

%% A client rebuilds the exact error type from the reason, not from the
%% gRPC status, which several types share.
error_round_trip_test() ->
    Cases = [
        task_not_cancelable,
        unsupported_operation,
        push_notification_not_supported,
        extended_agent_card_not_configured,
        version_not_supported,
        invalid_params,
        unauthenticated,
        rate_limited,
        internal_error
    ],
    lists:foreach(
        fun(Type) ->
            Error = barrel_a2a_error:new(Type),
            {Status, Message, Details} = livery_grpc_a2a_codec:error_status(Error),
            Back = livery_grpc_a2a_codec:error_from_status(Status, Message, Details),
            ?assertEqual(Type, barrel_a2a_error:type(Back)),
            ?assertEqual(barrel_a2a_error:message(Error), barrel_a2a_error:message(Back))
        end,
        Cases
    ).

error_details_round_trip_test() ->
    Error = barrel_a2a_error:invalid(<<"message.parts">>, <<"must not be empty">>),
    {Status, Message, Details} = livery_grpc_a2a_codec:error_status(Error),
    Back = livery_grpc_a2a_codec:error_from_status(Status, Message, Details),
    ?assertEqual(invalid_params, barrel_a2a_error:type(Back)),
    ?assertEqual(
        [
            #{
                <<"@type">> => <<"type.googleapis.com/google.rpc.ErrorInfo">>,
                <<"reason">> => <<"INVALID_PARAMS">>,
                <<"domain">> => <<"a2a-protocol.org">>
            },
            #{
                <<"@type">> => <<"type.googleapis.com/google.rpc.BadRequest">>,
                <<"fieldViolations">> => [
                    #{
                        <<"field">> => <<"message.parts">>,
                        <<"description">> => <<"must not be empty">>
                    }
                ]
            }
        ],
        barrel_a2a_error:details(Back)
    ).

error_info_metadata_test() ->
    Error = barrel_a2a_error:new(version_not_supported, <<"nope">>, [
        barrel_a2a_error:error_info(version_not_supported, #{<<"supportedVersions">> => <<"1.0">>})
    ]),
    {Status, Message, Details} = livery_grpc_a2a_codec:error_status(Error),
    Back = livery_grpc_a2a_codec:error_from_status(Status, Message, Details),
    ?assertMatch(
        [#{<<"metadata">> := #{<<"supportedVersions">> := <<"1.0">>}} | _],
        barrel_a2a_error:details(Back)
    ).

%% With no details the type can only come from the gRPC status.
error_without_details_test() ->
    Error = livery_grpc_a2a_codec:error_from_status(not_found, <<"gone">>, undefined),
    ?assertEqual(task_not_found, barrel_a2a_error:type(Error)),
    ?assertEqual(<<"gone">>, barrel_a2a_error:message(Error)).

error_with_unparseable_details_test() ->
    Error = livery_grpc_a2a_codec:error_from_status(
        internal, <<"boom">>, <<"not protobuf at all">>
    ),
    ?assertEqual(internal_error, barrel_a2a_error:type(Error)),
    ?assertEqual(<<"boom">>, barrel_a2a_error:message(Error)).
