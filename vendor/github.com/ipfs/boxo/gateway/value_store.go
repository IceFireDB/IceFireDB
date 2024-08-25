package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p/core/routing"
)

type remoteValueStore struct {
	httpClient *http.Client
	gatewayURL []string
	rand       *rand.Rand
}

// NewRemoteValueStore creates a new [routing.ValueStore] backed by one or more
// gateways that support IPNS Record requests. See the [Trustless Gateway]
// specification for more details. You can optionally pass your own [http.Client].
//
// [Trustless Gateway]: https://specs.ipfs.tech/http-gateways/trustless-gateway/
func NewRemoteValueStore(gatewayURL []string, httpClient *http.Client) (routing.ValueStore, error) {
	if len(gatewayURL) == 0 {
		return nil, errors.New("missing gateway URLs to which to proxy")
	}

	if httpClient == nil {
		httpClient = newRemoteHTTPClient()
	}

	return &remoteValueStore{
		gatewayURL: gatewayURL,
		httpClient: httpClient,
		rand:       rand.New(rand.NewSource(time.Now().Unix())),
	}, nil
}

func (ps *remoteValueStore) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (ps *remoteValueStore) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(k))
	if err != nil {
		return nil, err
	}

	return ps.fetch(ctx, name)
}

func (ps *remoteValueStore) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(k))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		v, err := ps.fetch(ctx, name)
		if err != nil {
			close(ch)
		} else {
			ch <- v
			close(ch)
		}
	}()

	return ch, nil
}

func (ps *remoteValueStore) fetch(ctx context.Context, name ipns.Name) ([]byte, error) {
	urlStr := fmt.Sprintf("%s/ipns/%s", ps.getRandomGatewayURL(), name.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipfs.ipns-record")
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from remote gateway: %s", resp.Status)
	}

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	rec, err := ipns.UnmarshalRecord(rb)
	if err != nil {
		return nil, err
	}

	err = ipns.ValidateWithName(rec, name)
	if err != nil {
		return nil, err
	}

	return rb, nil
}

func (ps *remoteValueStore) getRandomGatewayURL() string {
	return ps.gatewayURL[ps.rand.Intn(len(ps.gatewayURL))]
}
