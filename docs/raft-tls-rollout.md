# Raft TLS Rollout

Raft RPCs travel over `http://`, so everything raft replicates — every key, every
value, and the cluster shared secret itself — is readable by anyone who can
observe the network between two nodes. TLS on the raft port closes that. This
document is how to turn it on without stopping the cluster.

## What TLS adds, and what it does not

The cluster shared secret already authenticates the caller: a peer that cannot
present an accepted secret is refused. What the secret cannot do is hide itself,
which `docs/raft-secret-rollout.md` states directly — an adversary who reads the
wire lifts the secret out of any raft RPC and replays it.

TLS is what removes that adversary. Encrypting the connection makes the secret
unreadable in transit, so the two features are complements: TLS decides who can
*read* raft traffic, the secret decides who can *use* the raft port. Turning on
TLS is not a reason to stop configuring the secret.

TLS here proves the server's identity to the caller, not the caller's identity
to the server: each node presents a certificate, and no node asks its peer for
one. A process that can reach the TLS port still completes the handshake — it
just cannot get past the secret check afterwards. Client certificates are a
separate change and are not part of this rollout.

## The two ports

Every node listens on two raft ports for the whole migration:

| Port | Address | Serves |
|:---- |:------- |:------ |
| plaintext | `raft_api_port` | the raft service, unchanged |
| TLS | `raft_tls_port` | the same raft service, over TLS |

Both listeners run the same raft service and both apply the same shared-secret
check, so the choice of port changes the transport and nothing else.

A node's TLS port is its own to choose. What it publishes to its peers is a
whole address, `raft_advertise_host` and that port, kept in the node's own
record in the cluster membership beside the raft address already there, so a
peer reads where to dial rather than computing it. No value has to match across
the cluster, and several nodes on one host are no different from several nodes
on several hosts. Publishing an address rather than a port also leaves room to
serve TLS on a host of its own later, without changing what peers read.

That published address is also the entire dialing decision. A node dials a peer
over TLS when both of these hold, and dials plaintext otherwise:

- the dialing node has a CA to verify the peer against, that is
  `raft_tls_client_root_ca_cert` is set; and
- the peer publishes a TLS address.

Because the second half is answered per peer, out of data the cluster already
replicates, a node never has to guess whether a peer speaks TLS, and the nodes
may be upgraded in any order. There is also no attempt-and-fall-back: a failed
TLS connection is never retried in plaintext. A fallback would let anyone who
can drop one packet turn the encryption off.

## Configuration

All keys live under `[raft_config]`. Every node both serves raft and dials its
peers, and each key belongs to one of those two halves: `raft_tls_server_*` and
`raft_tls_port` are read by the listener, `raft_tls_client_*` by the code that
dials peers.

| Key | Meaning |
|:--- |:------- |
| `raft_tls_server_cert` | Path to this node's certificate chain, in PEM. |
| `raft_tls_server_key` | Path to that certificate's private key, in PEM. |
| `raft_tls_port` | The port this node's TLS listener binds, published to peers paired with `raft_advertise_host`. |
| `raft_tls_client_root_ca_cert` | Path to the CA this node verifies its peers' certificates against. |
| `raft_tls_client_domain_name` | The name this node expects in a peer's certificate. Optional. |

```toml
[raft_config]
raft_tls_server_cert = "/etc/databend-meta/tls/node.crt"
raft_tls_server_key = "/etc/databend-meta/tls/node.key"
raft_tls_port = 29004
raft_tls_client_root_ca_cert = "/etc/databend-meta/tls/ca.crt"
raft_tls_client_domain_name = "meta.internal"
```

Neither half of the node has an on/off switch of its own, because configuring it
is the switch. The listener runs when the certificate, the key and the port are
all present; leaving any of them out keeps the node on plaintext alone. Dialing
over TLS takes only the CA, since a node without one has nothing to verify a
peer against and a node with one has no reason to refuse.

Every key is unset by default, and a half-configured node is refused at startup
rather than started into a state that fails later:

- a certificate without its key, or a key without its certificate;
- `raft_tls_client_domain_name` with no `raft_tls_client_root_ca_cert`, which
  would leave the name never consulted;
- any of the four path-or-name keys set to an empty string, which is a config
  source setting a key to nothing rather than leaving it out.

Refusing early matters more here than for most configuration, for the reason
given under [Diagnosing a failure](#diagnosing-a-failure): a TLS problem reaches
raft as a peer that appears to be down.

### The certificate

With `raft_tls_client_domain_name` set, a node verifies its peers against that
name rather than against the address it dialed. One certificate valid for that
single name then works for the whole cluster, and the name does not have to
resolve in DNS. Issue the certificate for the name you put in that key, and put
the same name on every node.

Leaving the name unset falls back to ordinary address-based verification, which
instead requires each node's certificate to cover the address its peers dial it
on. That is workable where nodes have stable names, at the cost of one
certificate per node.

Expiry is a cluster-wide event, not a per-node one: certificates issued together
expire together, and the cluster loses every raft connection at the same moment
with no warning in raft's own view of the world. Track the expiry date outside
the cluster and renew before it.

Renewing a leaf certificate signed by the same CA is a plain rolling restart at
any point, with no coordination. Changing the CA is not covered here.

## The upgrade: one rolling restart

Deploy the new binary with the certificate, the key, the TLS port and the CA
set. Restart the nodes one at a time.

Each restarted node starts answering on its TLS port and publishes its TLS
address to the cluster. Peers that have a CA move to TLS for that node as soon
as they see the published address; peers still on the old binary, or running
without a CA, keep using its plaintext port, which never closed. A node that has
not restarted yet publishes no TLS address and is dialed in plaintext by
everyone.

The sweep may run in any order, and pausing it halfway leaves a working cluster
with a mix of encrypted and plaintext connections. When it finishes, every raft
connection in the cluster is encrypted, and the plaintext listener is still open
but unused.

Publishing is what a node already does at every startup: it re-registers its
record once the cluster has a leader, which is how a changed gRPC advertise
address takes effect today, and the TLS address rides along. That write goes
through raft, so a node restarting while the cluster has no leader listens on
TLS before its address becomes visible to anyone. Peers dial it in plaintext
during that window and move to TLS once the write commits. This needs no
action.

One asymmetry lasts as long as the sweep does. A node still on the old binary
drops the published address while applying the record, because its `Node` has no
such field. That costs nothing directly, since a node without a CA dials
plaintext anyway. It does mean that a node on the new binary which installs a
snapshot built by an old node loses every TLS address it had learned, and goes
back to dialing those peers in plaintext until each of them restarts and
publishes again. The plaintext listeners and the shared secret both still apply
while that lasts, and no node can erase an address once none of them runs the
old binary.

### Confirming the upgrade is complete

The dialing decision is made from the published node records, so those records
are what to check: every member should carry a TLS address. A member that does
not is a member everyone still talks to in plaintext, whatever its own
configuration file says.

Whether a listener really answers is a separate question from whether its
address is published, and `openssl s_client` is how to settle it for one node:

```bash
openssl s_client -connect NODE:TLS_PORT -servername meta.internal </dev/null
```

## Step 2 (optional, later): close the plaintext listener

The open plaintext listener is what keeps rollback cheap, so closing it is
deliberately a decision of its own, taken after the upgrade has been stable for
a while rather than as part of it. Its precondition is that every member
publishes a TLS address and every node has a CA — that is, that nothing in the
cluster still has a reason to dial plaintext.

Until this step, the raft port still accepts unencrypted connections from
anything that can reach it, and the shared secret is what stops those.

## Rollback

Stopping one node from dialing TLS is a local change: unset
`raft_tls_client_root_ca_cert` and restart it. It goes back to dialing every
peer in plaintext, and no other node is affected.

**Withdrawing a TLS listener is the direction that needs care.** A node that has
published a TLS address keeps that address in the cluster's records until it
publishes the withdrawal, and its peers keep dialing it in the meantime. Restart
the node with `raft_tls_port` unset and let it publish the withdrawal; do not
remove the listener in any way that leaves the published address behind.

**Rolling a node back to a pre-TLS binary has the same trap**, since that binary
neither listens on the TLS port nor knows how to withdraw it. Withdraw the
published address first, while still on the new binary, then downgrade.

After step 2, restore the plaintext listener before anything else, since a
cluster whose nodes only listen on TLS cannot accept a node that only dials
plaintext.

## Diagnosing a failure

`status_to_unreachable_at()` in `crates/server/service/src/network.rs` turns
every transport error into `Unreachable` without inspecting it, so raft never
learns that a TLS handshake failed. It sees a peer that is down.

The reason survives only on the dialing side, in its own log. A node that is
being dialed learns nothing at all, because the connection never becomes a
request. So diagnose this from the node that is dialing, never from the node
that looks unreachable.

## What this does not touch

The client-facing gRPC port and the admin HTTP port are separate listeners with
their own TLS settings, and nothing here changes either of them.
`docs/network-exposure.md` describes what each port requires of a caller.
