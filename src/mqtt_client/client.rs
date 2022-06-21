use super::{Error, Incoming, MqttOptions};
use bytes::{Bytes, BytesMut};
use mqttbytes::{
    v4::{read, Connect, ConnectReturnCode, Login, Packet, Publish},
    QoS,
};
use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

#[allow(dead_code)]
pub struct Network {
    socket: Box<dyn N>,
    read: BytesMut,
    max_incoming_size: usize,
}

#[allow(dead_code)]
impl Network {
    pub fn new(socket: impl N + 'static, max_incoming_size: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
        }
    }

    /// Reads more than 'required' bytes to frame a packet into self.read buffer
    pub fn read_bytes(&mut self, required: usize) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
            let mut buf = vec![0; required];
            let read = self.socket.read(&mut buf)?;
            if 0 == read {
                return if self.read.is_empty() {
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "connection closed by peer",
                    ))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "connection reset by peer",
                    ))
                };
            }
            self.read.extend_from_slice(&buf[..read]);

            total_read += read;
            if total_read >= required {
                return Ok(total_read);
            }
        }
    }

    pub fn read(&mut self) -> Result<Incoming, Error> {
        loop {
            let required = match read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                Err(mqttbytes::Error::InsufficientBytes(required)) => required,
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()).into())
                }
            };

            self.read_bytes(required)?;
        }
    }

    pub fn connect(&mut self, connect: Connect) -> Result<usize, Error> {
        let mut write = BytesMut::new();
        let len = match connect.write(&mut write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()).into()),
        };

        self.socket.write_all(&write[..])?;

        Ok(len)
    }

    pub fn write(&mut self, write: &mut BytesMut) -> Result<(), io::Error> {
        if write.is_empty() {
            return Ok(());
        }

        self.socket.write(&write[..])?;
        write.clear();
        Ok(())
    }
}

pub trait N: Read + Write + Send + Unpin {}
impl<T> N for T where T: Read + Write + Send + Unpin {}

#[allow(dead_code)]
pub struct Client {
    network: Network,
    options: MqttOptions,
}

const MAX_PACKET_SIZE: usize = 1024 * 1024;

#[allow(dead_code)]
impl Client {
    pub fn new(options: MqttOptions) -> Result<Self, io::Error> {
        let s = format!("{}:{}", options.broker_addr, options.port);
        let socket_addr: SocketAddr = s.parse().unwrap();
        let socket =
            TcpStream::connect_timeout(&socket_addr, Duration::from_secs(options.conn_timeout))?;
        let network = Network::new(socket, MAX_PACKET_SIZE);

        Ok(Self { network, options })
    }

    pub fn handshake(&mut self) -> Result<Incoming, Error> {
        let keep_alive = self.options.keep_alive().as_secs() as u16;
        let clean_session = self.options.clean_session();
        let last_will = self.options.last_will();

        let mut connect = Connect::new(self.options.client_id());
        connect.keep_alive = keep_alive;
        connect.clean_session = clean_session;
        connect.last_will = last_will;

        if let Some((username, password)) = self.options.credentials() {
            let login = Login::new(username, password);
            connect.login = Some(login);
        }

        self.network.connect(connect)?;

        let packet = match self.network.read()? {
            Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => {
                Packet::ConnAck(connack)
            }
            Incoming::ConnAck(connack) => {
                let error = format!("Broker rejected. Reason = {:?}", connack.code);
                return Err(io::Error::new(io::ErrorKind::InvalidData, error).into());
            }

            packet => {
                let error = format!("Expecting connack. Received = {:?}", packet);
                return Err(io::Error::new(io::ErrorKind::InvalidData, error).into());
            }
        };

        Ok(packet)
    }

    pub fn publish_bytes<S>(
        &mut self,
        topic: S,
        qos: QoS,
        // retain: bool,
        payload: Bytes,
    ) -> Result<(), mqttbytes::Error>
    where
        S: Into<String>,
    {
        let mut buf = BytesMut::new();
        let publish = Publish::from_bytes(topic, qos, payload);

        // publish.retain = retain;
        // publish.pkid = self.next_pkid();
        publish.write(&mut buf)?;
        self.network.write(&mut buf).unwrap();

        Ok(())
    }

    // http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    // Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    // fn next_pkid(&mut self) -> u16 {
    //   1
    // }
}
