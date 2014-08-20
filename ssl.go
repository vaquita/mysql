package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

// sslConnect establishes a SSL connection with the server.
func (c *Conn) sslConnect() error {
	var (
		cert     tls.Certificate
		certPool *x509.CertPool
		pemCerts []byte
		conn     *tls.Conn
		err      error
	)

	if c.p.sslCA != "" {
		certPool = x509.NewCertPool()
		if pemCerts, err = ioutil.ReadFile(c.p.sslCA); err != nil {
			return err
		} else {
			certPool.AppendCertsFromPEM(pemCerts)
		}
	}

	if cert, err = tls.LoadX509KeyPair(c.p.sslCert, c.p.sslKey); err != nil {
		return err
	}

	config := tls.Config{Certificates: []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            certPool}

	conn = tls.Client(c.conn, &config)

	if err = conn.Handshake(); err != nil {
		return err
	}

	// update the connection handle
	c.conn = conn
	return nil
}
