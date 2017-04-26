-module(tut5_send).
-export([send_msg/2]).

% Example Usage:
%   This module sends messages from your car's computer. The topic you use is
%   in the format: <<"system.severity">>
%
%   1. erl -pa */ebin
%   2. tut5_send:send_msg(<<"engine.info">>, <<"ok">>).
%   3. tut5_send:send_msg(<<"transmission.critical">>, <<"low fluid">>).
%   4. tut5_send:send_msg(<<"exhaust.warning">>, <<"ignore me">>).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

send_msg(Topic, Msg) ->
    {Connection, Channel} = create_queue("localhost"),
    amqp_channel:cast(Channel,
                      #'basic.publish'{exchange = <<"topic_logs">>, routing_key = Topic},
                      #amqp_msg{payload = Msg}),
    io:format(" [x] Sent ~s:'~s'.~n", [Topic, Msg]),
    close_connection(Connection, Channel),
    ok.

create_queue(Host) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"topic_logs">>,
                                                   type = <<"topic">>}),
    {Connection, Channel}.

close_connection(Connection, Channel) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
