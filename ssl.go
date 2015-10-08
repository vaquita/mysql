/*
  Copyright (C) 2015 Nirbhay Choubey

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
  USA
*/

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
			return myError(ErrSSLConnection, err)
		} else {
			certPool.AppendCertsFromPEM(pemCerts)
		}
	}

	if cert, err = tls.LoadX509KeyPair(c.p.sslCert, c.p.sslKey); err != nil {
		return myError(ErrSSLConnection, err)
	}

	config := tls.Config{Certificates: []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            certPool}

	conn = tls.Client(c.conn, &config)

	if err = conn.Handshake(); err != nil {
		return myError(ErrSSLConnection, err)
	}

	// update the connection handle
	c.conn = conn
	return nil
}
