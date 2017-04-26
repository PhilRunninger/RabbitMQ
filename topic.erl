%%% vim:foldmethod=marker
%
% Sample Run:
%
% $ erlc topic.erl                                          # compile the program
% $ erl -pa */ebin                                          # start the Erlang shell, adding the RabbitMQ libraries to the code path.
%
% 1> topic:start_link().                                    % starts the gen_server.
% {ok,<0.60.0>}
% 2> topic:monitor_msg(<<"*.critical">>).                   % tells the gen_server to listen for critical messages from any component.
%  [x] Listening for *.critical messages...
% ok
% 3> topic:monitor_msg(<<"engine.*">>).                     % tells the gen_server to listen for any message from the engine.
%  [x] Listening for engine.* messages...
% ok
% 4> topic:send_msg(<<"engine.info">>, <<"ok">>).           % let someone know the engine is ok.
%  [x] Sent engine.info:'ok'.
% ok
%  [x] Received engine.info:'ok'.
% 5> topic:send_msg(<<"transmission.info">>, <<"ok">>).     % let someone know the transmission is ok. (No one cares.)
%  [x] Sent transmission.info:'ok'.
% ok
% 6> topic:send_msg(<<"transmission.critical">>, <<"ok">>). % let someone know the tranny's gone kablooey. (Now they care.)
%  [x] Sent transmission.critical:'ok'.
% ok
%  [x] Received transmission.critical:'ok'.
% 7>

-module(topic).  % {{{1

-behaviour(gen_server).

%% API
-export([ start_link/0, monitor_msg/1, send_msg/2 ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {connection, channel}).

%%% API   {{{1

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

monitor_msg(Topic) ->
    gen_server:call(?SERVER, {monitor_msg, Topic}).

send_msg(Topic, Msg) ->
    gen_server:call(?SERVER, {send_msg, Topic, Msg}).

%%% gen_server callbacks   {{{1

init([]) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"topic_logs">>, type = <<"topic">>}),
    {ok, #state{connection=Connection, channel=Channel}}.

handle_call({monitor_msg, Topic}, _From, #state{channel=Channel}=State) ->
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Channel, #'queue.bind'{exchange = <<"topic_logs">>, routing_key = Topic, queue = Queue}),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    io:format(" [x] Listening for ~s messages...~n", [Topic]),
    {reply, ok, State};
handle_call({send_msg, Topic, Msg}, _From, #state{channel=Channel}=State) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{exchange = <<"topic_logs">>, routing_key = Topic},
                      #amqp_msg{payload = Msg}),
    io:format(" [x] Sent ~s:'~s'.~n", [Topic, Msg]),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({#'basic.deliver'{routing_key = RoutingKey}, #amqp_msg{payload = Body}}, State) ->
    io:format(" [x] Received ~s:'~s'.~n", [RoutingKey,Body]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions   {{{1
