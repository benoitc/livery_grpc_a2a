-module(livery_grpc_a2a_codec).
-moduledoc """
Conversion between gpb message maps and A2A JSON objects.

The A2A engine (`barrel_a2a_server_core`) speaks one shape for every
binding: the A2A JSON object, with binary camelCase keys, enums as their
full protobuf names, and timestamps as ISO 8601 text. The gRPC binding
speaks gpb maps: snake_case atom keys, tagged oneofs, `bytes` as
binaries, `google.protobuf.Timestamp` as `#{seconds, nanos}`. This
module is the translation, in both directions, for every message in
`a2a_pb`.

```erlang
Json = livery_grpc_a2a_codec:to_json('Task', #{id => <<"t1">>, ...}),
Pb   = livery_grpc_a2a_codec:to_pb('Task', Json).
```

It is descriptor driven: field names, types, oneof membership and
presence rules come from `a2a_pb:fetch_msg_def/1` at runtime, so a
change to `proto/a2a.proto` needs no change here. Five message types are
special cased because ProtoJSON gives them a shorthand form:
`google.protobuf.Struct` and `Value` and `ListValue` become plain JSON,
`Timestamp` becomes ISO 8601 text, `Empty` becomes an empty object.

The mapping follows canonical ProtoJSON, which is what the A2A
specification uses:

- A field with implicit presence (a plain proto3 field) is omitted from
  the JSON when it holds its type default; a field with explicit
  presence (`optional`) is written whenever it is set, `0` included.
- A protobuf map is omitted when empty.
- A repeated field is always written, as `[]` when empty. Canonical
  ProtoJSON would omit it, but the A2A schema marks several arrays
  required (`ListTasksResponse.tasks`, `AgentCard.skills`), and an empty
  array is the honest rendering of an empty list. It costs nothing on
  the wire: the protobuf encoding of an empty repeated field is no
  bytes either way.
- `bytes` is base64 with padding; a doubly-encoded integral value is
  written as an integer.
- Timestamps go through `barrel_a2a_time`, so they carry millisecond
  precision (`2024-03-15T10:15:00.000Z`) like every other binding. A
  finer input precision is truncated, not rejected.

The error helpers convert a `barrel_a2a_error:error()` to and from the
`google.rpc.Status` that rides in `grpc-status-details-bin`.
""".

-export([to_json/2, to_pb/2]).
-export([error_status/1, error_details/1, error_from_status/3]).
-export([proto/0, status_proto/0]).

-export_type([json/0]).

-type json() :: null | boolean() | number() | binary() | [json()] | #{binary() => json()}.

%% The gpb module for lf.a2a.v1.
-define(PROTO, a2a_pb).
%% The gpb module for the google.rpc types in grpc-status-details-bin.
-define(STATUS_PROTO, a2a_status_pb).

-define(ERROR_INFO_TYPE, <<"type.googleapis.com/google.rpc.ErrorInfo">>).
-define(BAD_REQUEST_TYPE, <<"type.googleapis.com/google.rpc.BadRequest">>).

-doc "The gpb module carrying the `lf.a2a.v1` messages.".
-spec proto() -> module().
proto() -> ?PROTO.

-doc "The gpb module carrying `google.rpc.Status` and its detail types.".
-spec status_proto() -> module().
status_proto() -> ?STATUS_PROTO.

%%====================================================================
%% gpb map -> A2A JSON
%%====================================================================

-doc """
Convert a decoded gpb message map to its A2A JSON object.

`Msg` is the message name in `a2a_pb` (`'Task'`, `'StreamResponse'`,
...). Fields holding their type default are omitted, as ProtoJSON
requires.
""".
-spec to_json(atom(), map()) -> json().
to_json('Struct', Pb) ->
    struct_to_json(Pb);
to_json('Value', Pb) ->
    value_to_json(Pb);
to_json('ListValue', Pb) ->
    list_value_to_json(Pb);
to_json('Timestamp', Pb) ->
    timestamp_to_json(Pb);
to_json(Msg, Pb) when is_map(Pb) ->
    lists:foldl(fun(Field, Acc) -> field_to_json(Field, Pb, Acc) end, #{}, fields(Msg)).

%% A oneof def carries `fields` and no `type`; the value in the message
%% map is `{MemberName, Value}`.
-spec field_to_json(map(), map(), map()) -> map().
field_to_json(#{name := Name, fields := Members}, Pb, Acc) ->
    case maps:find(Name, Pb) of
        {ok, {Tag, Value}} ->
            case member(Members, Tag) of
                {ok, #{type := Type}} -> Acc#{json_key(Tag) => term_to_json(Type, Value)};
                error -> Acc
            end;
        _ ->
            Acc
    end;
field_to_json(#{name := Name} = Field, Pb, Acc) ->
    case maps:find(Name, Pb) of
        {ok, Value} -> put_json(Field, Value, Acc);
        error -> Acc
    end.

-spec put_json(map(), term(), map()) -> map().
put_json(#{type := {map, _KeyType, _ValueType}}, Value, Acc) when map_size(Value) =:= 0 ->
    %% ProtoJSON omits an empty map field.
    Acc;
put_json(#{name := Name, type := {map, KeyType, ValueType}}, Value, Acc) ->
    Entries = maps:fold(
        fun(K, V, A) -> A#{map_key_to_json(KeyType, K) => term_to_json(ValueType, V)} end,
        #{},
        Value
    ),
    Acc#{json_key(Name) => Entries};
put_json(#{name := Name, occurrence := repeated, type := Type}, Value, Acc) ->
    Acc#{json_key(Name) => [term_to_json(Type, V) || V <- Value]};
put_json(#{name := Name, occurrence := defaulty, type := Type}, Value, Acc) ->
    %% Implicit presence: the type default is indistinguishable from
    %% unset, so ProtoJSON leaves it out.
    case is_default(Type, Value) of
        true -> Acc;
        false -> Acc#{json_key(Name) => term_to_json(Type, Value)}
    end;
put_json(#{name := Name, type := Type}, Value, Acc) ->
    %% Explicit presence: present in the map means set, even at 0.
    Acc#{json_key(Name) => term_to_json(Type, Value)}.

-spec term_to_json(term(), term()) -> json().
term_to_json({msg, Msg}, Value) -> to_json(Msg, Value);
term_to_json({enum, Enum}, Value) -> enum_to_json(Enum, Value);
term_to_json(bytes, Value) -> base64:encode(Value);
term_to_json(double, Value) -> number_to_json(Value);
term_to_json(float, Value) -> number_to_json(Value);
term_to_json(_Type, Value) -> Value.

-spec enum_to_json(atom(), atom() | integer()) -> binary() | integer().
enum_to_json(_Enum, Value) when is_integer(Value) -> Value;
enum_to_json(_Enum, Value) when is_atom(Value) -> atom_to_binary(Value, utf8).

%% A double that holds an integral value is written as an integer, the
%% canonical ProtoJSON rendering (`1`, not `1.0`).
-spec number_to_json(number()) -> number().
number_to_json(Value) when is_float(Value) ->
    Truncated = trunc(Value),
    case Truncated == Value of
        true -> Truncated;
        false -> Value
    end;
number_to_json(Value) ->
    Value.

-spec map_key_to_json(term(), term()) -> binary().
map_key_to_json(string, Key) -> Key;
map_key_to_json(_Type, Key) when is_integer(Key) -> integer_to_binary(Key);
map_key_to_json(_Type, Key) when is_atom(Key) -> atom_to_binary(Key, utf8).

-spec is_default(term(), term()) -> boolean().
is_default({msg, _}, _Value) -> false;
is_default({enum, Enum}, Value) -> enum_value(Enum, Value) =:= 0;
is_default(string, <<>>) -> true;
is_default(bytes, <<>>) -> true;
is_default(bool, false) -> true;
is_default(_Type, Value) when is_number(Value) -> Value == 0;
is_default(_Type, _Value) -> false.

-spec enum_value(atom(), atom() | integer()) -> integer() | undefined.
enum_value(_Enum, Value) when is_integer(Value) ->
    Value;
enum_value(Enum, Value) ->
    case ?PROTO:find_enum_def(Enum) of
        error -> undefined;
        Symbols -> enum_lookup(Symbols, Value)
    end.

%% gpb lists an enum's symbols as `{Symbol, Value, Opts}`.
-spec enum_lookup([{atom(), integer(), list()}], atom()) -> integer() | undefined.
enum_lookup([{Value, Number, _Opts} | _], Value) -> Number;
enum_lookup([_ | Rest], Value) -> enum_lookup(Rest, Value);
enum_lookup([], _Value) -> undefined.

%%====================================================================
%% A2A JSON -> gpb map
%%====================================================================

-doc """
Convert an A2A JSON object to a gpb message map ready for `encode_msg`.

Unknown JSON keys are ignored, as ProtoJSON requires. A `null` is taken
as "field not set" everywhere except inside a `google.protobuf.Value`,
where it is the null value.
""".
-spec to_pb(atom(), json()) -> map().
to_pb('Struct', Json) ->
    #{fields => maps:map(fun(_K, V) -> json_to_value(V) end, object(Json))};
to_pb('Value', Json) ->
    json_to_value(Json);
to_pb('ListValue', Json) ->
    #{values => [json_to_value(V) || V <- list(Json)]};
to_pb('Timestamp', Json) ->
    json_to_timestamp(Json);
to_pb(Msg, Json) when is_map(Json) ->
    lists:foldl(fun(Field, Acc) -> field_to_pb(Field, Json, Acc) end, #{}, fields(Msg));
to_pb(Msg, Json) ->
    fail({not_an_object, Msg, Json}).

-spec field_to_pb(map(), map(), map()) -> map().
field_to_pb(#{name := Name, fields := Members}, Json, Acc) ->
    case first_present(Members, Json) of
        {ok, #{name := Tag, type := Type}, Value} ->
            Acc#{Name => {Tag, json_to_term(Type, Value)}};
        error ->
            Acc
    end;
field_to_pb(#{name := Name} = Field, Json, Acc) ->
    case lookup(Name, Json) of
        {ok, null} -> Acc;
        {ok, Value} -> Acc#{Name => json_to_field(Field, Value)};
        error -> Acc
    end.

%% The first oneof member the object carries wins; a `null` member is
%% skipped so it cannot mask a later one.
-spec first_present([map()], map()) -> {ok, map(), json()} | error.
first_present([], _Json) ->
    error;
first_present([#{name := Name} = Field | Rest], Json) ->
    case lookup(Name, Json) of
        {ok, null} -> first_present(Rest, Json);
        {ok, Value} -> {ok, Field, Value};
        error -> first_present(Rest, Json)
    end.

-spec json_to_field(map(), json()) -> term().
json_to_field(#{type := {map, KeyType, ValueType}}, Value) ->
    maps:fold(
        fun(K, V, Acc) -> Acc#{map_key_to_pb(KeyType, K) => json_to_term(ValueType, V)} end,
        #{},
        object(Value)
    );
json_to_field(#{occurrence := repeated, type := Type}, Value) ->
    [json_to_term(Type, V) || V <- list(Value)];
json_to_field(#{type := Type}, Value) ->
    json_to_term(Type, Value).

-spec json_to_term(term(), json()) -> term().
json_to_term({msg, Msg}, Value) -> to_pb(Msg, Value);
json_to_term({enum, Enum}, Value) -> json_to_enum(Enum, Value);
json_to_term(bytes, Value) -> decode_base64(Value);
json_to_term(string, Value) when is_binary(Value) -> Value;
json_to_term(bool, Value) when is_boolean(Value) -> Value;
json_to_term(Type, Value) when Type =:= double; Type =:= float -> to_float(Value);
json_to_term(Type, Value) -> to_integer(Type, Value).

-spec json_to_enum(atom(), json()) -> atom() | integer().
json_to_enum(_Enum, Value) when is_integer(Value) ->
    Value;
json_to_enum(Enum, Value) when is_binary(Value) ->
    Symbol = binary_to_atom(Value, utf8),
    case enum_value(Enum, Symbol) of
        undefined -> fail({unknown_enum_value, Enum, Value});
        _Number -> Symbol
    end;
json_to_enum(Enum, Value) ->
    fail({unknown_enum_value, Enum, Value}).

%% ProtoJSON writes base64 with padding but accepts the URL-safe
%% alphabet and missing padding too.
-spec decode_base64(json()) -> binary().
decode_base64(Value) when is_binary(Value) ->
    Standard = binary:replace(
        binary:replace(Value, <<"-">>, <<"+">>, [global]), <<"_">>, <<"/">>, [global]
    ),
    Padded =
        case byte_size(Standard) rem 4 of
            0 -> Standard;
            2 -> <<Standard/binary, "==">>;
            3 -> <<Standard/binary, "=">>;
            _ -> fail({invalid_base64, Value})
        end,
    try
        base64:decode(Padded)
    catch
        _:_ -> fail({invalid_base64, Value})
    end;
decode_base64(Value) ->
    fail({invalid_base64, Value}).

%% ProtoJSON allows 64-bit integers to travel as strings.
-spec to_integer(term(), json()) -> integer().
to_integer(_Type, Value) when is_integer(Value) ->
    Value;
to_integer(Type, Value) when is_binary(Value) ->
    try
        binary_to_integer(Value)
    catch
        _:_ -> fail({not_an_integer, Type, Value})
    end;
to_integer(Type, Value) when is_float(Value) ->
    case trunc(Value) == Value of
        true -> trunc(Value);
        false -> fail({not_an_integer, Type, Value})
    end;
to_integer(Type, Value) ->
    fail({not_an_integer, Type, Value}).

-spec to_float(json()) -> float() | nan | infinity | '-infinity'.
to_float(Value) when is_number(Value) -> float(Value);
to_float(<<"NaN">>) -> nan;
to_float(<<"Infinity">>) -> infinity;
to_float(<<"-Infinity">>) -> '-infinity';
to_float(Value) -> fail({not_a_number, Value}).

-spec map_key_to_pb(term(), binary()) -> term().
map_key_to_pb(string, Key) -> Key;
map_key_to_pb(bool, <<"true">>) -> true;
map_key_to_pb(bool, <<"false">>) -> false;
map_key_to_pb(Type, Key) -> to_integer(Type, Key).

%%====================================================================
%% google.protobuf.Struct, Value, ListValue, Timestamp
%%====================================================================

-spec struct_to_json(map()) -> #{binary() => json()}.
struct_to_json(#{fields := Fields}) -> maps:map(fun(_K, V) -> value_to_json(V) end, Fields);
struct_to_json(_) -> #{}.

-spec value_to_json(map()) -> json().
value_to_json(#{kind := {null_value, _}}) -> null;
value_to_json(#{kind := {number_value, N}}) -> number_to_json(N);
value_to_json(#{kind := {string_value, S}}) -> S;
value_to_json(#{kind := {bool_value, B}}) -> B;
value_to_json(#{kind := {struct_value, S}}) -> struct_to_json(S);
value_to_json(#{kind := {list_value, L}}) -> list_value_to_json(L);
value_to_json(_) -> null.

-spec list_value_to_json(map()) -> [json()].
list_value_to_json(#{values := Values}) -> [value_to_json(V) || V <- Values];
list_value_to_json(_) -> [].

-spec json_to_value(json()) -> map().
json_to_value(null) -> #{kind => {null_value, 'NULL_VALUE'}};
json_to_value(Value) when is_boolean(Value) -> #{kind => {bool_value, Value}};
json_to_value(Value) when is_number(Value) -> #{kind => {number_value, float(Value)}};
json_to_value(Value) when is_binary(Value) -> #{kind => {string_value, Value}};
json_to_value(Value) when is_list(Value) -> #{kind => {list_value, to_pb('ListValue', Value)}};
json_to_value(Value) when is_map(Value) -> #{kind => {struct_value, to_pb('Struct', Value)}}.

%% Timestamps use `barrel_a2a_time`, so the gRPC binding writes the same
%% millisecond-precision text as the JSON-RPC and HTTP+JSON bindings.
-spec timestamp_to_json(map()) -> binary().
timestamp_to_json(Pb) ->
    Seconds = maps:get(seconds, Pb, 0),
    Nanos = maps:get(nanos, Pb, 0),
    barrel_a2a_time:to_iso(Seconds * 1000 + Nanos div 1000000).

-spec json_to_timestamp(json()) -> map().
json_to_timestamp(Iso) when is_binary(Iso) ->
    case barrel_a2a_time:from_iso(Iso) of
        {ok, Ms} ->
            Seconds = floor(Ms / 1000),
            #{seconds => Seconds, nanos => (Ms - Seconds * 1000) * 1000000};
        error ->
            fail({invalid_timestamp, Iso})
    end;
json_to_timestamp(Value) ->
    fail({invalid_timestamp, Value}).

%%====================================================================
%% Errors: barrel_a2a_error <-> google.rpc.Status
%%====================================================================

-doc """
The gRPC trailer triple for an A2A error.

Returns `{Status, Message, Details}` for
`{error, {Status, Message, Details}}`, where `Details` is an encoded
`google.rpc.Status` for the `grpc-status-details-bin` trailer.
""".
-spec error_status(barrel_a2a_error:error()) -> {atom(), binary(), binary()}.
error_status(Error) ->
    Status = barrel_a2a_error:grpc_status(barrel_a2a_error:type(Error)),
    {Status, barrel_a2a_error:message(Error), error_details(Error)}.

-doc """
Encode an A2A error as `google.rpc.Status` bytes.

The details always start with an `ErrorInfo` carrying the A2A reason
(`TASK_NOT_FOUND`, ...) and the `a2a-protocol.org` domain, so a client
recovers the exact error type from the status. A `BadRequest` detail is
carried through when the error has one.
""".
-spec error_details(barrel_a2a_error:error()) -> binary().
error_details(Error) ->
    Type = barrel_a2a_error:type(Error),
    Code = livery_grpc_status:code(barrel_a2a_error:grpc_status(Type)),
    Details = with_error_info(Type, barrel_a2a_error:details(Error)),
    ?STATUS_PROTO:encode_msg(
        #{
            code => Code,
            message => barrel_a2a_error:message(Error),
            details => [Any || D <- Details, {ok, Any} <- [detail_to_any(D)]]
        },
        'Status'
    ).

%% The error type must survive the trip even when the server attached no
%% details of its own.
-spec with_error_info(atom(), [map()]) -> [map()].
with_error_info(Type, Details) ->
    case
        lists:any(fun(D) -> maps:get(<<"@type">>, D, undefined) =:= ?ERROR_INFO_TYPE end, Details)
    of
        true -> Details;
        false -> [barrel_a2a_error:error_info(Type, #{}) | Details]
    end.

%% Only the two detail types the A2A specification defines are encoded;
%% anything else would need its own descriptor to become an Any.
-spec detail_to_any(map()) -> {ok, map()} | skip.
detail_to_any(#{<<"@type">> := ?ERROR_INFO_TYPE} = Detail) ->
    Info = #{
        reason => maps:get(<<"reason">>, Detail, <<>>),
        domain => maps:get(<<"domain">>, Detail, <<>>),
        metadata => string_map(maps:get(<<"metadata">>, Detail, #{}))
    },
    {ok, #{
        type_url => ?ERROR_INFO_TYPE,
        value => ?STATUS_PROTO:encode_msg(Info, 'ErrorInfo')
    }};
detail_to_any(#{<<"@type">> := ?BAD_REQUEST_TYPE} = Detail) ->
    Violations = [
        #{
            field => maps:get(<<"field">>, V, <<>>),
            description => maps:get(<<"description">>, V, <<>>),
            reason => maps:get(<<"reason">>, V, <<>>)
        }
     || V <- maps:get(<<"fieldViolations">>, Detail, []), is_map(V)
    ],
    {ok, #{
        type_url => ?BAD_REQUEST_TYPE,
        value => ?STATUS_PROTO:encode_msg(#{field_violations => Violations}, 'BadRequest')
    }};
detail_to_any(_Detail) ->
    skip.

%% ErrorInfo metadata is map<string, string>; render anything else the
%% caller put there as text rather than dropping the entry.
-spec string_map(term()) -> #{binary() => binary()}.
string_map(Map) when is_map(Map) ->
    maps:fold(fun(K, V, Acc) -> Acc#{to_text(K) => to_text(V)} end, #{}, Map);
string_map(_) ->
    #{}.

%% A structured value (the list of supported versions, say) is rendered
%% as JSON rather than as an Erlang term, so a client in any language can
%% read it.
-spec to_text(term()) -> binary().
to_text(V) when is_binary(V) ->
    V;
to_text(V) when is_atom(V) ->
    atom_to_binary(V, utf8);
to_text(V) when is_integer(V) ->
    integer_to_binary(V);
to_text(V) ->
    try
        iolist_to_binary(json:encode(V))
    catch
        _:_ -> iolist_to_binary(io_lib:format("~0p", [V]))
    end.

-doc """
Rebuild an A2A error from a gRPC failure.

`Details` is the raw `grpc-status-details-bin` value, or `undefined`
when the server sent none; the A2A error type then comes from the gRPC
status alone.
""".
-spec error_from_status(atom(), binary(), binary() | undefined) -> barrel_a2a_error:error().
error_from_status(Status, Message, undefined) ->
    barrel_a2a_error:new(type_from_grpc(Status), fallback_message(Status, Message));
error_from_status(Status, Message, Details) ->
    case decode_status(Details) of
        {ok, #{details := Anys} = Decoded} ->
            Objects = [O || A <- Anys, {ok, O} <- [any_to_detail(A)]],
            Text = first_message([maps:get(message, Decoded, <<>>), Message]),
            barrel_a2a_error:new(
                type_from_details(Objects, type_from_grpc(Status)),
                fallback_message(Status, Text),
                Objects
            );
        error ->
            barrel_a2a_error:new(type_from_grpc(Status), fallback_message(Status, Message))
    end.

-spec decode_status(binary()) -> {ok, map()} | error.
decode_status(Details) ->
    try
        {ok, ?STATUS_PROTO:decode_msg(Details, 'Status')}
    catch
        _:_ -> error
    end.

-spec any_to_detail(map()) -> {ok, map()} | skip.
any_to_detail(#{type_url := ?ERROR_INFO_TYPE, value := Value}) ->
    try ?STATUS_PROTO:decode_msg(Value, 'ErrorInfo') of
        Info ->
            Base = #{
                <<"@type">> => ?ERROR_INFO_TYPE,
                <<"reason">> => maps:get(reason, Info, <<>>),
                <<"domain">> => maps:get(domain, Info, <<>>)
            },
            {ok, put_non_empty(<<"metadata">>, maps:get(metadata, Info, #{}), Base)}
    catch
        _:_ -> skip
    end;
any_to_detail(#{type_url := ?BAD_REQUEST_TYPE, value := Value}) ->
    try ?STATUS_PROTO:decode_msg(Value, 'BadRequest') of
        #{field_violations := Violations} ->
            {ok, #{
                <<"@type">> => ?BAD_REQUEST_TYPE,
                <<"fieldViolations">> => [violation_to_json(V) || V <- Violations]
            }}
    catch
        _:_ -> skip
    end;
any_to_detail(_Any) ->
    skip.

-spec violation_to_json(map()) -> map().
violation_to_json(Violation) ->
    Base = #{
        <<"field">> => maps:get(field, Violation, <<>>),
        <<"description">> => maps:get(description, Violation, <<>>)
    },
    put_non_empty(<<"reason">>, maps:get(reason, Violation, <<>>), Base).

-spec put_non_empty(binary(), term(), map()) -> map().
put_non_empty(_Key, <<>>, Map) -> Map;
put_non_empty(_Key, Value, Map) when Value =:= #{} -> Map;
put_non_empty(Key, Value, Map) -> Map#{Key => Value}.

-spec first_message([binary()]) -> binary().
first_message([<<>> | Rest]) -> first_message(Rest);
first_message([Message | _]) -> Message;
first_message([]) -> <<>>.

-spec fallback_message(atom(), binary()) -> binary().
fallback_message(Status, <<>>) -> atom_to_binary(Status, utf8);
fallback_message(_Status, Message) -> Message.

%% An ErrorInfo reason names the A2A error type exactly; the gRPC status
%% is only the fallback because several types share one status.
-spec type_from_details([map()], atom()) -> atom().
type_from_details(Details, Default) ->
    Reasons = [
        R
     || #{<<"@type">> := ?ERROR_INFO_TYPE, <<"reason">> := R} <- Details, is_binary(R), R =/= <<>>
    ],
    case Reasons of
        [Reason | _] -> type_from_reason(Reason, Default);
        [] -> Default
    end.

-spec type_from_reason(binary(), atom()) -> atom().
type_from_reason(Reason, Default) ->
    case [T || T <- a2a_error_types(), barrel_a2a_error:reason(T) =:= Reason] of
        [Type | _] -> Type;
        [] -> Default
    end.

-spec a2a_error_types() -> [atom()].
a2a_error_types() ->
    [
        task_not_found,
        task_not_cancelable,
        push_notification_not_supported,
        unsupported_operation,
        content_type_not_supported,
        invalid_agent_response,
        extended_agent_card_not_configured,
        extension_support_required,
        version_not_supported,
        parse_error,
        invalid_request,
        method_not_found,
        invalid_params,
        internal_error,
        unauthenticated,
        permission_denied,
        rate_limited,
        unavailable,
        timeout
    ].

-spec type_from_grpc(atom()) -> atom().
type_from_grpc(not_found) -> task_not_found;
type_from_grpc(invalid_argument) -> invalid_params;
type_from_grpc(failed_precondition) -> unsupported_operation;
type_from_grpc(unauthenticated) -> unauthenticated;
type_from_grpc(permission_denied) -> permission_denied;
type_from_grpc(resource_exhausted) -> rate_limited;
type_from_grpc(unavailable) -> unavailable;
type_from_grpc(deadline_exceeded) -> timeout;
type_from_grpc(unimplemented) -> method_not_found;
type_from_grpc(cancelled) -> transport;
type_from_grpc(_Status) -> internal_error.

%%====================================================================
%% Internals
%%====================================================================

-spec fields(atom()) -> [map()].
fields(Msg) ->
    ?PROTO:fetch_msg_def(Msg).

-spec member([map()], atom()) -> {ok, map()} | error.
member([#{name := Name} = Field | _Rest], Name) -> {ok, Field};
member([_Field | Rest], Name) -> member(Rest, Name);
member([], _Name) -> error.

%% Look a field up by its JSON name, then by its proto name: ProtoJSON
%% parsers must accept both.
-spec lookup(atom(), map()) -> {ok, json()} | error.
lookup(Name, Json) ->
    case maps:find(json_key(Name), Json) of
        {ok, _} = Found -> Found;
        error -> maps:find(atom_to_binary(Name, utf8), Json)
    end.

%% snake_case atom -> lowerCamelCase binary (`context_id` -> `contextId`).
-spec json_key(atom()) -> binary().
json_key(Name) ->
    [First | Rest] = binary:split(atom_to_binary(Name, utf8), <<"_">>, [global]),
    iolist_to_binary([First | [capitalize(Part) || Part <- Rest]]).

-spec capitalize(binary()) -> binary().
capitalize(<<>>) -> <<>>;
capitalize(<<C, Rest/binary>>) when C >= $a, C =< $z -> <<(C - 32), Rest/binary>>;
capitalize(Part) -> Part.

-spec object(json()) -> map().
object(Value) when is_map(Value) -> Value;
object(Value) -> fail({not_an_object, Value}).

-spec list(json()) -> [json()].
list(Value) when is_list(Value) -> Value;
list(Value) -> fail({not_a_list, Value}).

-spec fail(term()) -> no_return().
fail(Reason) -> error({a2a_codec, Reason}).
