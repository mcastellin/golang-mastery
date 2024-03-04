# Domain Name Server

This project contains a toy DNS server implementation that can either serve DNS type A records from an in-memory storage
or forward DNS requests to an upstream server (`8.8.8.8`).

## About

This is NOT A PRODUCTION-GRADE APPLICATION, but it's been an opportunity to learn how to handle UDP packets by decoding and encoding
raw byte streams. In this implementation I decided not to use any pre-existing DNS library on purpose.

## Run the DNS server with Docker

Build and run the Docker image:

```bash
docker build -t dns-server .
docker run --rm --name dns-server --publish "53:53/udp" dns-server
```

Run a test DNS query:

```bash
dig @localhost blog.acme.com

# ; <<>> DiG 9.10.6 <<>> @localhost blog.acme.com
# ; (2 servers found)
# ;; global options: +cmd
# ;; Got answer:
# ;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 25421
# ;; flags: qr rd ra; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 1
# 
# ;; OPT PSEUDOSECTION:
# ; EDNS: version: 0, flags:; udp: 4096
# ;; QUESTION SECTION:
# ;blog.acme.com.                 IN      A
# 
# ;; ANSWER SECTION:
# blog.acme.com.          300     IN      A       127.0.0.1
# 
# ;; Query time: 0 msec
# ;; SERVER: 127.0.0.1#53(127.0.0.1)
# ;; WHEN: Mon Mar 04 16:27:58 CET 2024
# ;; MSG SIZE  rcvd: 71
```
