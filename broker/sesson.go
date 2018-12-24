package broker

import "github.com/eclipse/paho.mqtt.golang/packets"

func (b *Broker) getSession(cli *client, req *packets.ConnectPacket, resp *packets.ConnackPacket) error {
	// If CleanSession is set to 0, the server MUST resume communications with the
	// client based on state from the current session, as identified by the client
	// identifier. If there is no session associated with the client identifier the
	// server must create a new session.
	//
	// If CleanSession is set to 1, the client and server must discard any previous
	// session and start a new one. b session lasts as long as the network c
	// onnection. State data associated with b session must not be reused in any
	// subsequent session.

	var err error

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.

	if len(req.ClientIdentifier) == 0 {
		req.CleanSession = true
	}

	cid := req.ClientIdentifier

	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	if !req.CleanSession {
		if cli.session, err = b.sessionMgr.Get(cid); err == nil {
			resp.SessionPresent = true

			if err := cli.session.Update(req); err != nil {
				return err
			}
		}
	}

	// If CleanSession, or no existing session found, then create a new one
	if cli.session == nil {
		if cli.session, err = b.sessionMgr.New(cid); err != nil {
			return err
		}

		resp.SessionPresent = false

		if err := cli.session.Init(req); err != nil {
			return err
		}
	}

	return nil
}
