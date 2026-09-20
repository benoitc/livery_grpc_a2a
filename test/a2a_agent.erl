-module(a2a_agent).
-moduledoc """
Test fixture: the A2A agent the gRPC binding suite serves.

A reimplementation of barrel_a2a's own test agent, kept here so this
repository's suite does not depend on another project's test sources.
The behaviour is chosen by the text of the incoming message.
""".

-behaviour(barrel_a2a_handler).

-export([handle_message/2, handle_cancel/1]).

handle_message(Ctx, Message) ->
    Text = barrel_a2a_message:text(Message),
    case barrel_a2a_ctx:is_follow_up(Ctx) of
        true -> {ok, <<"thanks: ", Text/binary>>};
        false -> dispatch(Text, Ctx)
    end.

%% Answer with an artifact.
dispatch(<<"echo: ", Rest/binary>>, _Ctx) ->
    {ok, Rest};
%% Answer with a Message instead of a Task.
dispatch(<<"direct">>, _Ctx) ->
    {message, barrel_a2a_message:agent(<<"direct reply">>)};
%% A status update and two chunks of one artifact, so the event order is
%% observable on a stream.
dispatch(<<"stream">>, Ctx) ->
    ok = barrel_a2a_ctx:status(Ctx, working, #{message => <<"starting">>}),
    ok = barrel_a2a_ctx:artifact(Ctx, <<"part one ">>, #{
        artifact_id => <<"a1">>, name => <<"out">>
    }),
    ok = barrel_a2a_ctx:artifact(Ctx, <<"part two">>, #{
        artifact_id => <<"a1">>, append => true, last_chunk => true
    }),
    ok;
dispatch(<<"slow ", Ms/binary>>, _Ctx) ->
    timer:sleep(binary_to_integer(Ms)),
    {ok, <<"done">>};
%% Emits before it blocks, so a subscriber sees an event straight away
%% and the task is still running afterwards.
dispatch(<<"stream-slow ", Ms/binary>>, Ctx) ->
    ok = barrel_a2a_ctx:status(Ctx, working, #{message => <<"starting">>}),
    timer:sleep(binary_to_integer(Ms)),
    {ok, <<"done">>};
dispatch(<<"cancel-me">>, Ctx) ->
    ok = barrel_a2a_ctx:status(Ctx, working),
    wait_cancel(Ctx, 200);
dispatch(<<"ask">>, _Ctx) ->
    {input_required, <<"more?">>};
dispatch(<<"fail">>, _Ctx) ->
    {error, <<"boom">>};
dispatch(<<"data">>, Ctx) ->
    Parts = [
        barrel_a2a_part:data(#{<<"answer">> => 42}),
        barrel_a2a_part:file_url(<<"https://example.com/report.pdf">>, <<"application/pdf">>),
        barrel_a2a_part:file_bytes(<<1, 2, 3>>, <<"application/octet-stream">>, #{
            filename => <<"b.bin">>
        })
    ],
    ok = barrel_a2a_ctx:artifact(Ctx, Parts, #{name => <<"data">>}),
    ok;
dispatch(Other, _Ctx) ->
    {ok, <<"unknown: ", Other/binary>>}.

wait_cancel(_Ctx, 0) ->
    {ok, <<"never cancelled">>};
wait_cancel(Ctx, N) ->
    case barrel_a2a_ctx:cancelled(Ctx) of
        true ->
            ok;
        false ->
            timer:sleep(25),
            wait_cancel(Ctx, N - 1)
    end.

handle_cancel(_Ctx) ->
    ok.
