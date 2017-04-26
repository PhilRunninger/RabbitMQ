%%% vim:foldmethod=marker
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
