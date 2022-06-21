use std::{
    fmt::{self, Debug, Formatter},
    io,
    time::Duration,
};

pub mod client;
pub use mqttbytes::v4::*;
pub use mqttbytes::*;
pub type Incoming = Packet;

/// Current outgoing activity on the eventloop
#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(dead_code)]
pub enum Outgoing {
    /// Publish packet with packet identifier. 0 implies QoS 0
    Publish(u16),
    /// Subscribe packet with packet identifier
    Subscribe(u16),
    /// Unsubscribe packet with packet identifier
    Unsubscribe(u16),
    /// PubAck packet
    PubAck(u16),
    /// PubRec packet
    PubRec(u16),
    /// PubRel packet
    PubRel(u16),
    /// PubComp packet
    PubComp(u16),
    /// Ping request packet
    PingReq,
    /// Ping response packet
    PingResp,
    /// Disconnect packet
    Disconnect,
    /// Await for an ack for more outgoing progress
    AwaitAck(u16),
}

/// Key type for TLS authentication
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum Key {
    RSA(Vec<u8>),
    ECC(Vec<u8>),
}

#[derive(Clone)]
#[allow(dead_code)]
pub enum Transport {
    Tcp,
    Unix,
}

impl Default for Transport {
    fn default() -> Self {
        Self::tcp()
    }
}

#[allow(dead_code)]
impl Transport {
    /// Use regular tcp as transport (default)
    pub fn tcp() -> Self {
        Self::Tcp
    }

    #[cfg(unix)]
    pub fn unix() -> Self {
        Self::Unix
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct MqttOptions {
    /// broker address that you want to connect to
    broker_addr: String,
    /// broker port
    port: u16,
    // What transport protocol to use
    transport: Transport,
    /// keep alive time to send pingreq to broker when the connection is idle
    keep_alive: Duration,
    /// clean (or) persistent session
    clean_session: bool,
    /// client identifier
    client_id: String,
    /// username and password
    credentials: Option<(String, String)>,
    /// maximum incoming packet size (verifies remaining length of the packet)
    max_incoming_packet_size: usize,
    /// Maximum outgoing packet size (only verifies publish payload size)
    // TODO Verify this with all packets. This can be packet.write but message left in
    // the state might be a footgun as user has to explicitly clean it. Probably state
    // has to be moved to network
    max_outgoing_packet_size: usize,
    /// request (publish, subscribe) channel capacity
    request_channel_capacity: usize,
    /// Max internal request batching
    max_request_batch: usize,
    /// Minimum delay time between consecutive outgoing packets
    /// while retransmitting pending packets
    pending_throttle: Duration,
    /// maximum number of outgoing inflight messages
    inflight: u16,
    /// Last will that will be issued on unexpected disconnect
    last_will: Option<LastWill>,
    /// Connection timeout
    conn_timeout: u64,
}

#[allow(dead_code)]
impl MqttOptions {
    /// New mqtt options
    pub fn new<S: Into<String>, T: Into<String>>(id: S, host: T, port: u16) -> MqttOptions {
        let id = id.into();
        if id.starts_with(' ') || id.is_empty() {
            panic!("Invalid client id")
        }

        MqttOptions {
            broker_addr: host.into(),
            port,
            transport: Transport::tcp(),
            keep_alive: Duration::from_secs(60),
            clean_session: true,
            client_id: id,
            credentials: None,
            max_incoming_packet_size: 10 * 1024,
            max_outgoing_packet_size: 10 * 1024,
            request_channel_capacity: 10,
            max_request_batch: 0,
            pending_throttle: Duration::from_micros(0),
            inflight: 100,
            last_will: None,
            conn_timeout: 5,
        }
    }

    /// Broker address
    pub fn broker_address(&self) -> (String, u16) {
        (self.broker_addr.clone(), self.port)
    }

    pub fn set_last_will(&mut self, will: LastWill) -> &mut Self {
        self.last_will = Some(will);
        self
    }

    pub fn last_will(&self) -> Option<LastWill> {
        self.last_will.clone()
    }

    pub fn set_transport(&mut self, transport: Transport) -> &mut Self {
        self.transport = transport;
        self
    }

    pub fn transport(&self) -> Transport {
        self.transport.clone()
    }

    /// Set number of seconds after which client should ping the broker
    /// if there is no other data exchange
    pub fn set_keep_alive(&mut self, duration: Duration) -> &mut Self {
        if duration.as_secs() < 5 {
            panic!("Keep alives should be >= 5  secs");
        }

        self.keep_alive = duration;
        self
    }

    /// Keep alive time
    pub fn keep_alive(&self) -> Duration {
        self.keep_alive
    }

    /// Client identifier
    pub fn client_id(&self) -> String {
        self.client_id.clone()
    }

    /// Set client id
    pub fn set_client_id(&mut self, client_id: String) -> &mut Self {
        self.client_id = client_id;
        self
    }

    /// Set packet size limit for outgoing an incoming packets
    pub fn set_max_packet_size(&mut self, incoming: usize, outgoing: usize) -> &mut Self {
        self.max_incoming_packet_size = incoming;
        self.max_outgoing_packet_size = outgoing;
        self
    }

    /// Maximum packet size
    pub fn max_packet_size(&self) -> usize {
        self.max_incoming_packet_size
    }

    /// `clean_session = true` removes all the state from queues & instructs the broker
    /// to clean all the client state when client disconnects.
    ///
    /// When set `false`, broker will hold the client state and performs pending
    /// operations on the client when reconnection with same `client_id`
    /// happens. Local queue state is also held to retransmit packets after reconnection.
    pub fn set_clean_session(&mut self, clean_session: bool) -> &mut Self {
        self.clean_session = clean_session;
        self
    }

    /// Clean session
    pub fn clean_session(&self) -> bool {
        self.clean_session
    }

    /// Username and password
    pub fn set_credentials<S: Into<String>>(&mut self, username: S, password: S) -> &mut Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }

    /// Security options
    pub fn credentials(&self) -> Option<(String, String)> {
        self.credentials.clone()
    }

    /// Set request channel capacity
    pub fn set_request_channel_capacity(&mut self, capacity: usize) -> &mut Self {
        self.request_channel_capacity = capacity;
        self
    }

    /// Request channel capacity
    pub fn request_channel_capacity(&self) -> usize {
        self.request_channel_capacity
    }

    /// Enables throttling and sets outoing message rate to the specified 'rate'
    pub fn set_pending_throttle(&mut self, duration: Duration) -> &mut Self {
        self.pending_throttle = duration;
        self
    }

    /// Outgoing message rate
    pub fn pending_throttle(&self) -> Duration {
        self.pending_throttle
    }

    /// Set number of concurrent in flight messages
    pub fn set_inflight(&mut self, inflight: u16) -> &mut Self {
        if inflight == 0 {
            panic!("zero in flight is not allowed")
        }

        self.inflight = inflight;
        self
    }

    /// Number of concurrent in flight messages
    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// set connection timeout in secs
    pub fn set_connection_timeout(&mut self, timeout: u64) -> &mut Self {
        self.conn_timeout = timeout;
        self
    }

    /// get timeout in secs
    pub fn connection_timeout(&self) -> u64 {
        self.conn_timeout
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[allow(dead_code)]
pub enum OptionError {
    #[error("Unsupported URL scheme.")]
    Scheme,

    #[error("Missing client ID.")]
    ClientId,

    #[error("Invalid keep-alive value.")]
    KeepAlive,

    #[error("Invalid clean-session value.")]
    CleanSession,

    #[error("Invalid max-incoming-packet-size value.")]
    MaxIncomingPacketSize,

    #[error("Invalid max-outgoing-packet-size value.")]
    MaxOutgoingPacketSize,

    #[error("Invalid request-channel-capacity value.")]
    RequestChannelCapacity,

    #[error("Invalid max-request-batch value.")]
    MaxRequestBatch,

    #[error("Invalid pending-throttle value.")]
    PendingThrottle,

    #[error("Invalid inflight value.")]
    Inflight,

    #[error("Invalid conn-timeout value.")]
    ConnTimeout,

    #[error("Unknown option: {0}")]
    Unknown(String),
}

impl std::convert::TryFrom<url::Url> for MqttOptions {
    type Error = OptionError;

    fn try_from(url: url::Url) -> Result<Self, Self::Error> {
        use std::collections::HashMap;

        let broker_addr = url.host_str().unwrap_or_default().to_owned();

        let (transport, default_port) = match url.scheme() {
            "mqtts" | "ssl" => (Transport::Tcp, 8883),
            "mqtt" | "tcp" => (Transport::Tcp, 1883),
            _ => return Err(OptionError::Scheme),
        };

        let port = url.port().unwrap_or(default_port);

        let mut queries = url.query_pairs().collect::<HashMap<_, _>>();

        let keep_alive = Duration::from_secs(
            queries
                .get("keep_alive_secs")
                .map(|v| v.parse::<u64>().map_err(|_| OptionError::KeepAlive))
                .transpose()?
                .unwrap_or(60),
        );

        let client_id = queries.remove("client_id").unwrap_or("".into()).into();

        let clean_session = queries
            .remove("clean_session")
            .map(|v| v.parse::<bool>().map_err(|_| OptionError::CleanSession))
            .transpose()?
            .unwrap_or(true);

        let credentials = {
            match url.username() {
                "" => None,
                username => Some((
                    username.to_owned(),
                    url.password().unwrap_or_default().to_owned(),
                )),
            }
        };

        let max_incoming_packet_size = queries
            .remove("max_incoming_packet_size_bytes")
            .map(|v| {
                v.parse::<usize>()
                    .map_err(|_| OptionError::MaxIncomingPacketSize)
            })
            .transpose()?
            .unwrap_or(10 * 1024);

        let max_outgoing_packet_size = queries
            .remove("max_outgoing_packet_size_bytes")
            .map(|v| {
                v.parse::<usize>()
                    .map_err(|_| OptionError::MaxOutgoingPacketSize)
            })
            .transpose()?
            .unwrap_or(10 * 1024);

        let request_channel_capacity = queries
            .remove("request_channel_capacity_num")
            .map(|v| {
                v.parse::<usize>()
                    .map_err(|_| OptionError::RequestChannelCapacity)
            })
            .transpose()?
            .unwrap_or(10);

        let max_request_batch = queries
            .remove("max_request_batch_num")
            .map(|v| v.parse::<usize>().map_err(|_| OptionError::MaxRequestBatch))
            .transpose()?
            .unwrap_or(0);

        let pending_throttle = Duration::from_micros(
            queries
                .remove("pending_throttle_usecs")
                .map(|v| v.parse::<u64>().map_err(|_| OptionError::PendingThrottle))
                .transpose()?
                .unwrap_or(0),
        );

        let inflight = queries
            .remove("inflight_num")
            .map(|v| v.parse::<u16>().map_err(|_| OptionError::Inflight))
            .transpose()?
            .unwrap_or(100);

        let conn_timeout = queries
            .remove("conn_timeout_secs")
            .map(|v| v.parse::<u64>().map_err(|_| OptionError::ConnTimeout))
            .transpose()?
            .unwrap_or(5);

        if let Some((opt, _)) = queries.into_iter().next() {
            return Err(OptionError::Unknown(opt.into_owned()));
        }

        Ok(Self {
            broker_addr,
            port,
            transport,
            keep_alive,
            clean_session,
            client_id,
            credentials,
            max_incoming_packet_size,
            max_outgoing_packet_size,
            request_channel_capacity,
            max_request_batch,
            pending_throttle,
            inflight,
            last_will: None,
            conn_timeout,
        })
    }
}

// Implement Debug manually because ClientConfig doesn't implement it, so derive(Debug) doesn't
// work.
impl Debug for MqttOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("MqttOptions")
            .field("broker_addr", &self.broker_addr)
            .field("port", &self.port)
            .field("keep_alive", &self.keep_alive)
            .field("clean_session", &self.clean_session)
            .field("client_id", &self.client_id)
            .field("credentials", &self.credentials)
            .field("max_packet_size", &self.max_incoming_packet_size)
            .field("request_channel_capacity", &self.request_channel_capacity)
            .field("max_request_batch", &self.max_request_batch)
            .field("pending_throttle", &self.pending_throttle)
            .field("inflight", &self.inflight)
            .field("last_will", &self.last_will)
            .field("conn_timeout", &self.conn_timeout)
            .finish()
    }
}

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum Error {
    /// Io Error while state is passed to network
    #[error("Io error {0:?}")]
    Io(#[from] io::Error),
    /// Broker's error reply to client's connect packet
    #[error("Connect return code `{0:?}`")]
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("Mqtt serialization/deserialization error")]
    Deserialization(mqttbytes::Error),
}

impl From<mqttbytes::Error> for Error {
    fn from(e: mqttbytes::Error) -> Error {
        Error::Deserialization(e)
    }
}
