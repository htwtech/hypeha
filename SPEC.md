WSARB application

WSARB is a Rust binary, very high performant, to make a traffic arbitration between multible hyperliquid sources.

As initial configuration, application receives a number of websocket endpoints, for example ws://localhost:48001, ws://localhost:48002.
It connects to them and doing nothing.

On a separate note, it waits for user requests. They are coming in a form:

```
{"method": "subscribe","subscription": {"type": "l2Book","coin": "BTC"}}
```

The only changing thing here is coin - BTC (can be everything else).

User can have multiple subscriptions, they are all adding up.

On first subscription to unknown (not subscribed before) coin, app subscribes to it on all of the sources.

On each update, it 
a) updates the internal state
b) distributes the update to all of the clients subscribed

System is storing the most recent change for each coin (orderbook-coin). (by time field). So if the update from channel X is older it is just dropped.
(even it could be that the update was not even received from the newest channel, we don't care about stale data).

The service also has quite a simple built-in stats page.

For each data-connection, it tries to reconnect if disconnected. Disconnected sources is not a problem and service should continue working (HA)

For each data-connection: amount of received packets. If connected or not. Amount of disconnects. Avg times this connection provided the first result (higher time when current). If slower, some hystogram of delays comparing to the fastest source.

For each client connection: list of coins it subscribed to. IP address. Amount of traffic sent to that user.

Example of communication with data provider could be found in 48001.log
