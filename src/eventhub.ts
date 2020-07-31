/*
The MIT License (MIT)
Copyright (c) 2020 Ole Fredrik Skudsvik <ole.skudsvik@gmail.com>
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

import WebSocket from 'isomorphic-ws';

type RPCCallback = (err: string, message: string) => void;
type SubscriptionCallback = (message: MessageResult) => void;

enum RPCMethods {
  PUBLISH = "publish",
  SUBSCRIBE = 'subscribe',
  UNSUBSCRIBE = 'unsubscribe',
  UNSUBSCRIBE_ALL = 'unsubscribeAll',
  LIST = 'list',
  HISTORY = 'history',
  PING = 'ping',
  DISCONNECT = 'disconnect'
}

type Subscription = {
  topic: string
  callback: SubscriptionCallback
  lastRecvMessageId?: string
}

type RPCRequest = {
  id: number
  jsonrpc: string
  method: RPCMethods
  params: any
}

type WSMessage = {
  data: string
}

type RPCResponse = {
  id: number
  result?: any
  error?: any
}

type MessageResult = {
  id: any
  message: any
  topic: string
}

class ConnectionOptions {
  pingInterval: number = 10000;
  pingTimeout: number = 3000;
  maxFailedPings: number = 3;
  reconnectInterval: number = 10000;
  disablePingCheck: boolean = false;

  constructor(init?: Partial<ConnectionOptions>) {
    Object.assign(this, init);
  }
}

export default class Eventhub {
  private readonly _wsUrl: string;
  private _socket: WebSocket;
  private readonly _opts: ConnectionOptions;
  private _isConnected: boolean;

  private _rpcResponseCounter: number;
  private readonly _rpcCallbacks: Map<number, RPCCallback>;
  private readonly _subscriptionCallbacks: Map<number, Subscription>;
  private readonly _sentPings: Map<number, number>;

  private _pingTimer: any;
  private _pingTimeOutTimer: any;

  /**
   * Constructor (new Eventhub).
   * @param url Eventhub websocket url.
   * @param token Authentication token.
   * @param opts connection options
   */
  constructor(url: string, token?: string, opts?: Partial<ConnectionOptions>) {
    this._rpcResponseCounter = 0;
    this._rpcCallbacks = new Map();
    this._subscriptionCallbacks = new Map();
    this._sentPings = new Map();
    this._wsUrl = `${url}/?auth=${token}`;
    this._socket = undefined;
    this._isConnected = false;
    this._opts = new ConnectionOptions(opts);
  }

  /**
   * Connect to eventhub.
   * @returns Promise with true on success or error string on fail.
   */
  public connect(): Promise<any> {
    return new Promise(
      (resolve, reject) => {
        if (this._isConnected) {
          return resolve(true);
        }

        this._socket = new WebSocket(this._wsUrl);
        this._socket.onmessage = this._parseRPCResponse.bind(this);

        this._socket.onopen = () => {
          this._isConnected = true;

          if (!this._opts.disablePingCheck) {
            this._startPingMonitor();
          }

          resolve(true);
        };

        this._socket.onerror = (err) => {
          if (this._isConnected) {
            console.log("Eventhub WebSocket connection error:", err);
            this._isConnected = false;
            this._reconnect();
          } else {
            reject(err);
          }
        };

        this._socket.onclose = (err) => {
          if (this._isConnected) {
            this._isConnected = false;
            this._reconnect();
          }
        };
      });
  }

  /*
   * Try to reconnect in a loop until we succeed.
  */
  private _reconnect(): void {
    if (this._isConnected) return;
    const reconnectInterval = this._opts.reconnectInterval;

    if (this._socket.readyState != WebSocket.CLOSED && this._socket.readyState != WebSocket.CLOSING) {
      this._socket.close();
    }

    if (!this._opts.disablePingCheck) {
      clearInterval(this._pingTimer);
      clearInterval(this._pingTimeOutTimer);
      this._sentPings.clear();
    }

    this.connect().then(res => {
      const subscriptions = Array.from(this._subscriptionCallbacks.values());
      this._rpcResponseCounter = 0;
      this._rpcCallbacks.clear();
      this._subscriptionCallbacks.clear();

      for (let sub of subscriptions) {
        this.subscribe(sub.topic, sub.callback, {sinceEventId: sub.lastRecvMessageId});
      }
    }).catch(err => {
      setTimeout(this._reconnect.bind(this), reconnectInterval);
    });
  }

  /*
  * Send pings to the server on the configured interval.
  * Mark the client as disconnected if <_opts.maxFailedPings> pings fail.
  * Also trigger the reconnect procedure.
  */
  private _startPingMonitor(): void {
    const pingInterval = this._opts.pingInterval;
    const maxFailedPings = this._opts.maxFailedPings;

    // Ping server each <pingInterval> second.
    this._pingTimer = setInterval(() => {
      if (!this._isConnected) {
        return;
      }

      const pingRequest = this._getRPCRequest(RPCMethods.PING, []);

      this._sentPings.set(pingRequest.id, Date.now());

      this._sendRPCRequest(pingRequest).then(pong => {
        this._sentPings.delete(pingRequest.id);
      });
    }, pingInterval);

    // Check that our pings were successfully ponged by the server.
    // disconnect and reconnect if maxFailedPings is reached.
    this._pingTimeOutTimer = setInterval(() => {
      const now = Date.now();
      let failedPingCount = 0;

      for (const pingTimestamp of this._sentPings.values()) {
        if (now > (pingTimestamp + this._opts.pingTimeout)) {
          failedPingCount++;
        }
      }

      if (failedPingCount >= maxFailedPings) {
        this._isConnected = false;
        this._reconnect();
      }
    }, pingInterval);
  }

  /**
   * Send a RPC request to the connected Eventhub.
   * @param request What parameters to include with the call.
   * @return Promise with response or error.
   */
  private async _sendRPCRequest(request: RPCRequest): Promise<any> {
    if (this._socket.readyState != WebSocket.OPEN) {
      throw new Error("WebSocket is not connected.");
    }

    return new Promise((resolve, reject) => {
      try {
        this._socket.send(JSON.stringify(request));
        this._rpcCallbacks.set(request.id, (err: string, resp: any) => {
          err != null ? reject(err) : resolve(resp)
          // Remove the callback once it has been called.
          this._rpcCallbacks.delete(request.id);
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  /**
   * Parse incoming websocket response and call correct handlers.
   * @param response Response string.
   */
  private _parseRPCResponse(response: WSMessage): void {
    try {
      const rpcResponse = JSON.parse(response.data);

      if (!isRPCResponse(rpcResponse)) {
        return;
      }

      if (this._subscriptionCallbacks.has(rpcResponse.id) && isMessageResult(rpcResponse.result)) {
        const subscription = this._subscriptionCallbacks.get(rpcResponse.id);
        subscription.lastRecvMessageId = rpcResponse.result.id;
        subscription.callback(rpcResponse.result);
        return;
      }

      if (this._rpcCallbacks.has(rpcResponse.id)) {
        const rpcCallback = this._rpcCallbacks.get(rpcResponse.id);
        rpcCallback(rpcResponse.error, rpcResponse.result);
      }
    } catch (err) {
      console.log("Failed to parse websocket response:", err);
      return;
    }
  }

  /**
   * Check if we are already subscribed to a topic.
   * @param topic Topic.
   */
  public isSubscribed(topic: string) {
    for (const subscription of this._subscriptionCallbacks.values()) {
      if (subscription.topic === topic) return true;
    }
    return false;
  }

  /**
   * Subscribe to a topic pattern.
   * @param topic Topic to subscribe to.
   * @param callback Callback to call when we receive a message the subscribed topic.
   * @param opts Options to send with the request.
   * @returns Promise with success or callback.
   */
  public async subscribe(topic: string, callback: SubscriptionCallback, opts?: Object): Promise<any> {
    if (topic == "") {
      throw new Error("Topic cannot be empty.");
    }

    // First check if we are already subscribed to the topic.
    if (this.isSubscribed(topic)) {
      throw new Error(`Already subscribed to ${topic}`);
    }

    const subscribeRequest = this._getRPCRequest(RPCMethods.SUBSCRIBE, {topic, ...opts});

    this._subscriptionCallbacks.set(subscribeRequest.id, {topic, callback});

    return this._sendRPCRequest(subscribeRequest);
  }

  /**
   * Unsubscribe to one or more topic patterns.
   * @param topics Array of topics or single topic to unsubscribe from.
   */
  public unsubscribe(topics: Array<string> | string): void {
    const topicList = (typeof (topics) === 'string') ? [topics] : topics;

    if (!topicList.length) {
      return
    }

    for (const [id, subscription] of this._subscriptionCallbacks) {
      if (topicList.includes(subscription.topic)) {
        this._subscriptionCallbacks.delete(id);
      }
    }

    this._sendRPCRequest(this._getRPCRequest(RPCMethods.UNSUBSCRIBE, topicList));
  }

  /**
   * Unsubscribe from all topics.
   */
  public unsubscribeAll(): void {
    this._subscriptionCallbacks.clear();
    this._sendRPCRequest(this._getRPCRequest(RPCMethods.UNSUBSCRIBE_ALL, []));
  }

  /**
   * Publish a message.
   * @param topic Topic to publish to.
   * @param message Message to publish.
   * @param opts Options to send with the request.
   */
  public publish(topic: string, message: any, opts?: Object): Promise<any> {
    return this._sendRPCRequest(this._getRPCRequest(RPCMethods.PUBLISH, {topic, message, ...opts}));
  }

  /**
   * List all current subscriptions.
   * @return Array containing current subscribed topics.
   */
  public listSubscriptions(): Promise<any> {
    return this._sendRPCRequest(this._getRPCRequest(RPCMethods.LIST, []));
  }

  private _getRPCRequest(method: RPCMethods, params: any): RPCRequest {
    return {
      id: ++this._rpcResponseCounter,
      jsonrpc: "2.0",
      method: method,
      params: params,
    }
  }
}

// Type guards https://www.typescriptlang.org/docs/handbook/advanced-types.html#using-type-predicates
function isRPCResponse(response): response is RPCResponse {
  const rpcResponse = (response as RPCResponse);
  return rpcResponse.id != null && (rpcResponse.result != null || rpcResponse.error != null);
}

function isMessageResult(result: any): result is MessageResult {
  const messageResult = (result as MessageResult);
  return messageResult.message != null && messageResult.topic != null;
}
