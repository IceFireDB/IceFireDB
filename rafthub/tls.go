// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/tidwall/redlog/v2"
)

func parseTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	tlscfg := &tls.Config{
		Certificates: []tls.Certificate{pair},
	}
	for _, cert := range pair.Certificate {
		pcert, err := x509.ParseCertificate(cert)
		if err != nil {
			return nil, err
		}
		if len(pcert.DNSNames) > 0 {
			tlscfg.ServerName = pcert.DNSNames[0]
			break
		}
	}
	return tlscfg, nil
}

func tlsInit(conf Config, log *redlog.Logger) *tls.Config {
	if conf.TLSCertPath == "" || conf.TLSKeyPath == "" {
		return nil
	}
	tlscfg, err := parseTLSConfig(conf.TLSCertPath, conf.TLSKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	return tlscfg
}
